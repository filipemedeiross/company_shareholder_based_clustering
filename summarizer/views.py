from django.shortcuts import render

from summarizer.stats import get_statistics


def dashboard(request):
    return render(
        request,
        'summarizer/dashboard.html',
        {'stats': get_statistics()}
    )
