import logging

from braces.views import JsonRequestResponseMixin
from django.http import HttpResponse
from django.conf import settings
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.validators import ASCIIUsernameValidator
from django.core.validators import (EmailValidator, MaxLengthValidator,
                                    MinLengthValidator,
                                    ProhibitNullCharactersValidator,
                                    RegexValidator, ValidationError)
from django.views.generic.base import View

from core.models import Privilege


logger = logging.getLogger(__name__)


def chained_validators(validators, *args, field=None, **kwargs):
    for validator in validators:
        try:
            validator(*args, **kwargs)
        except ValidationError as exp:
            return False, {'field': field, 'message': str(exp)}
    return True, None

def validate_username(username):
    validators = [
        ASCIIUsernameValidator(message='Invalid characters.'),
        ProhibitNullCharactersValidator(message='Invalid characters'),
        MinLengthValidator(4, message='Username should be 4~16 characters long.'),
        MaxLengthValidator(16, message='Username should be 4~16 characters long.')
    ]
    return chained_validators(validators, username, field='username')

def validate_email(email):
    validators = [EmailValidator(message='Invalid email address.')]
    return chained_validators(validators, email, field='email')

def validate_password(password1, password2):
    if password1 != password2:
        return False, {'field': 'password2', 'message': 'Passwords should be the same.'}
    validators = [RegexValidator(regex=settings.USER_PASSWORD_VALIDATE_REGEX,
        message='Password should be 8~20 characters (include digits, letters or '
        '!@#$%^&*) long, have at least one number and one letter.')]
    return chained_validators(validators, password1, field='password')


class UserRegisterView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        username = self.request_json['username']
        result, message = validate_username(username)
        if not result:
            logger.info('User registration denied: Invalid username <%s>', username)
            return self.render_json_response({'success':False, 'info':message})
        email = self.request_json['email']
        result, message = validate_email(email)
        if not result:
            logger.info('User registration denied: Invalid email <%s>', email)
            return self.render_json_response({'success':False, 'info':message})
        password1 = self.request_json['password']
        password2 = self.request_json['password2']
        result, message = validate_password(password1, password2)
        if not result:
            logger.info('User registration denied: Invalid password')
            return self.render_json_response({'success':False, 'info':message})
        # check if username is already registered
        try:
            User.objects.get_by_natural_key(username)
        except User.DoesNotExist:
            pass
        else:
            logger.info('User registration denied: Already used username <%s>', username)
            message = {'field':'username', 'message':'The username has already been used.'}
            return self.render_json_response({'success':False, 'info':message})
        # Then we create it.
        try:
            user = User.objects.create_user(username, email=email, password=password1)
            Privilege.objects.create(uid=user.pk, username=username)
        except Exception as exp:
            logger.exception('Unknown exception caused user registration to fail')
            return self.render_json_response({'success':False, 'info':{'field':None}})
        else:
            logger.info('Done user registration: Username <%s>, Email <%s>', username, email)
            return self.render_json_response({'success':True})


class UserAutoLoginView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            response = {'logined': True, 'username': request.user.username,
                'plevel': request.session.get('USER_PLEVEL', 0)}
            logger.info('Auto logined: %s', request.user.username)
            return self.render_json_response(response)
        else:
            return self.render_json_response({'logined': False})


class UserLoginView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        username = self.request_json['username']
        password = self.request_json['password']
        # abuse prevention
        if len(username) > 20 or len(password) > 25:
            return self.render_json_response({'logined': False})
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            plevel = Privilege.check_level(user.pk)
            request.session['USER_PLEVEL'] = plevel
            response = {'logined': True, 'username': username, 'plevel': plevel}
            logger.info('Logined: %s', username)
            return self.render_json_response(response)
        else:
            logger.info('Login denied: %s', username)
            return self.render_json_response({'logined': False})


class UserLogoutView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            logger.info('Logouted: %s', request.user.username)
        logout(request)
        return self.render_json_response({'logined': False})


class ProtectedFilesView(View):

    def get(self, request, *args, **kwargs):
        if request.user.is_authenticated and request.session.get('USER_PLEVEL', 0) > 0:
            response = HttpResponse(status=200)
            response['Content-Type'] = ''
            path = request.path[11:] # /protected/...
            response['X-Accel-Redirect'] = '/protectedmedia/' + path
        else:
            response = HttpResponse(status=403)
        return response
