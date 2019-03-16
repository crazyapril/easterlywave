from django.urls import path
from blog.views import ArticleView, ArticleItemsView, TagItemsView

urlpatterns = [
    path('articles', ArticleItemsView.as_view()),
    path('article', ArticleView.as_view()),
    path('tags', TagItemsView.as_view())
]
