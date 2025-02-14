from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action
from .models import Book, Author
from .serializers import BookSerializer, AuthorSerializer
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.pagination import PageNumberPagination


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = "page_size"
    max_page_size = 100


class BookViewSet(viewsets.ModelViewSet):
    queryset = Book.objects.all()
    serializer_class = BookSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["author"]
    pagination_class = StandardResultsSetPagination

    @action(detail=True, methods=["post"])
    def buy(self, request, pk=None):
        book = self.get_object()
        if book.count > 0:
            book.count -= 1
            book.save()
            return Response({"status": "Book purchased"}, status=status.HTTP_200_OK)
        return Response(
            {"error": "Book out of stock"}, status=status.HTTP_400_BAD_REQUEST
        )


class AuthorViewSet(viewsets.ModelViewSet):
    queryset = Author.objects.all()
    serializer_class = AuthorSerializer

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)
        for author_data in serializer.data:
            author = Author.objects.get(id=author_data["id"])
            author_data["books"] = list(author.books.values_list("title", flat=True))
        return Response(serializer.data)
