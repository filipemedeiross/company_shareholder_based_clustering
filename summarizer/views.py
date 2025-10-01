from django.shortcuts import render
from .duckdb_connection import get_statistics

def dashboard(request):
    """View para exibir o dashboard com estatísticas."""
    stats = get_statistics()
    return render(request, 'summarizer/dashboard.html', {'stats': stats})
