from braces.views import JsonRequestResponseMixin
from django.views.generic.base import View
from django.core.cache import cache

from blog.models import Article, Tag
from tools.cache import Key


class ArticleItemsView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        tag = self.request_json['tagFilter']
        if tag == '所有':
            queryset = Article.get_articles(self.request_json['length'],
                self.request_json['offset'])
        else:
            queryset = Article.get_articles_by_tag(tag, self.request_json['length'],
                self.request_json['offset'])
        response = {'articles': [a.to_short_json() for a in queryset]}
        return self.render_json_response(response)


class TagItemsView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        tags = cache.get(Key.BLOG_TAGS)
        if tags is None:
            tags = [{'name': '所有', 'val': Article.objects.all().count()}]
            for tag in Tag.objects.all():
                tags.append({'name': tag.name, 'val': tag.articles.all().count()})
            cache.set(Key.BLOG_TAGS, tags, 86400)
        return self.render_json_response({'tags': tags})


class ArticleView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        pk = int(self.request_json['pk'])
        try:
            article = Article.objects.get(pk=pk)
        except Article.DoesNotExist:
            return self.render_json_response({'error':True})
        else:
            article.viewed()
        return self.render_json_response({'article': article.to_full_json()})
