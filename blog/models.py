import re
from datetime import timedelta

import markdown
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.utils.html import strip_tags

from tools.cache import Key
from tools.naturaltime import naturaltime


class Article(models.Model):

    title = models.CharField(max_length=64, verbose_name='标题')
    content = models.TextField(verbose_name='正文')
    created_time = models.DateTimeField(verbose_name='创建时间', auto_now_add=True)
    modified_time = models.DateTimeField(verbose_name='修改时间', auto_now=True)
    excerpt = models.CharField(max_length=150, blank=True)
    tags = models.ManyToManyField('Tag', related_name='articles')
    author = models.ForeignKey(User, related_name='articles', on_delete=models.PROTECT)

    class Meta:

        ordering = ['-created_time', 'author']

    def __str__(self):
        return self.title

    def make_excerpt(self):
        html = markdown.markdown(self.content)
        first_paragraph = re.search(r'<p>.*?</p>', html)
        excerpt = strip_tags(first_paragraph.group(0))
        excerpt = ' '.join(excerpt.split())
        if len(excerpt) > 140:
            excerpt = excerpt[:140] + '...'
        return excerpt

    def viewed(self):
        try:
            self.view.views += 1
        except ObjectDoesNotExist:
            ArticleView.objects.create(article=self, views=1)
        else:
            self.view.save()

    def save(self, **kwargs):
        self.excerpt = self.make_excerpt()
        cache.delete(Key.BLOG_TAGS)
        return super().save(**kwargs)

    @classmethod
    def get_articles(cls, length=5, offset=0):
        return cls.objects.all()[offset:offset+length]

    @classmethod
    def get_articles_by_tag(cls, tag_name, length=5, offset=0):
        tag = Tag.objects.get(name=tag_name)
        return tag.articles.all()[offset:offset+length]

    def to_short_json(self):
        json = {
            'pk': self.pk,
            'title': self.title,
            'excerpt': self.excerpt,
            'created': naturaltime(self.created_time),
            'tags': [str(tag) for tag in self.tags.all()]
        }
        return json

    def to_full_json(self):
        json = {
            'pk': self.pk,
            'title': self.title,
            # UGLY !!!
            'created': (self.created_time + timedelta(hours=8)).strftime('%Y/%m/%d %H:%M'),
            'modified': self.modified_time.strftime('%Y/%m/%d %H:%M'),
            'content': self.content,
            'author': self.author.username,
            'tags': [str(tag) for tag in self.tags.all()],
        }
        return json


class Tag(models.Model):

    name = models.CharField(max_length=16, verbose_name='名称')

    class Meta:

        ordering = ['name']

    def __str__(self):
        return self.name


class ArticleView(models.Model):

    article = models.OneToOneField(Article, on_delete=models.CASCADE,
        primary_key=True, related_name='view')
    views = models.PositiveIntegerField(default=0, blank=True, verbose_name='浏览量')

    def __str__(self):
        return '{} | {}次'.format(self.article.title[:10], self.views)
