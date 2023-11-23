from django.db import models

# Represents a user
class User(models.Model):
    username = models.CharField(max_length = 255, default = None)       # Username

# Represents a table for annotation
class Table(models.Model):
    name = models.CharField(max_length = 255, default = None)    # Name of query

# Represents an input query
class Query(models.Model):
    name = models.CharField(max_length = 255, default = None)            # Name of query

# Represents an annotation for a query and table
class Annotation(models.Model):
    query = models.ForeignKey(Query, to_field = 'id', on_delete = models.CASCADE, default = -1) # Reference to Query
    table = models.ForeignKey(Table, to_field = 'id', on_delete = models.CASCADE, default = -1) # Reference to Table
    score = models.IntegerField()                                                               # Annotation score
    user = models.ForeignKey(User, to_field = 'id', on_delete = models.CASCADE, default = -1)   # Reference to User

# Represents a table annotation task
class Task(models.Model):
    query = models.ForeignKey(Query, to_field = 'id', on_delete = models.CASCADE, default = -1) # Reference to Query
    tables = models.CharField(max_length = 1024, default = None)                                # Array of table names for this task

# Represents some work, where a user has been assigned a query and a table to annotate
class Work(models.Model):
    user = models.ForeignKey(User, to_field = 'id', on_delete = models.CASCADE, default = -1)   # Reference to user
    task = models.ForeignKey(Task, to_field = 'id', on_delete = models.CASCADE, default = -1)   # Reference to Task

# Work that has already been completed
class CompletedWork(models.Model):
    work = models.ForeignKey(Work, to_field = 'id', on_delete = models.CASCADE, default = -1)   # Reference to user
    elapsed_time = models.FloatField(default = -1.0)                                            # Elapsed time in seconds to complete assigned task