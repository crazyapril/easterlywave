from django.contrib import admin

from blog.models import Article, ArticleView, Tag


@admin.register(Article)
class ArticleAdmin(admin.ModelAdmin):

    list_display = ['title', 'created_time', 'modified_time', 'author']


admin.site.register([ArticleView, Tag])
