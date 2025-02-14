from rest_framework import serializers
from .models import Book, Author


class AuthorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Author
        fields = ["id", "first_name", "last_name"]


class BookSerializer(serializers.ModelSerializer):
    author = AuthorSerializer(read_only=True)
    author_id = serializers.PrimaryKeyRelatedField(
        queryset=Author.objects.all(), write_only=True
    )

    class Meta:
        model = Book
        fields = ["id", "title", "author", "author_id", "count"]

    def create(self, validated_data):
        validated_data["author"] = validated_data.pop("author_id")
        return super().create(validated_data)

    def update(self, instance, validated_data):
        if "author_id" in validated_data:
            validated_data["author"] = validated_data.pop("author_id")
        return super().update(instance, validated_data)
