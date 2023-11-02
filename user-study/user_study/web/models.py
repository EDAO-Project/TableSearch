from django.db import models

class Score(models.Model):
    score = models.IntegerField()
    table_id = models.CharField(max_length = 255)
    id = models.IntegerField(primary_key = True)
    username = models.CharField(max_length = 255)

class Table(models.Model):
    table_id = models.CharField(max_length = 255, primary_key = True)
    query_id = models.CharField(max_length = 255)
    scores = models.ForeignKey(Score, to_field = 'id', on_delete = models.CASCADE)