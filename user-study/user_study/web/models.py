from django.db import models

class User(models.Model):
    id = models.AutoField(primary_key = True, auto_created = True)      # Referential ID
    username = models.CharField(max_length = 255, db_index = True)      # Username

class Annotation(models.Model):
    query_id = models.CharField(max_length = 255, db_index = True)   # ID of query
    table_id = models.CharField(max_length = 255)   # ID of table
    score = models.IntegerField()                   # Annotation score
    user = models.ForeignKey(User, to_field = 'id', on_delete = models.CASCADE, default = -1)