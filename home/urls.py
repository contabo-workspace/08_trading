from django.urls import path
from . import views

app_name = 'home'

urlpatterns = [
    path('', views.HomePageView.as_view(), name='index'),
    path('api/dashboard/', views.DashboardDataView.as_view(), name='dashboard-data'),
]
