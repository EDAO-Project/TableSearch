from django.db import models

# Represents a user
class User(models.Model):
    username = models.CharField(max_length = 255, default = '')       # Username

# Represents a table for annotation
class Table(models.Model):
    name = models.CharField(max_length = 255, default = '')    # Name of query

# Represents an input query
class Query(models.Model):
    name = models.CharField(max_length = 255, default = '')            # Name of query

# Represents an annotation for a query and table
class Annotation(models.Model):
    query = models.ForeignKey(Query, on_delete = models.CASCADE, default = -1, null = True) # Reference to Query
    table = models.ForeignKey(Table, on_delete = models.CASCADE, default = -1, null = True) # Reference to Table
    score = models.IntegerField()                                                               # Annotation score
    user = models.ForeignKey(User, on_delete = models.CASCADE, default = -1, null = True)   # Reference to User

# Represents a table annotation task
class Task(models.Model):
    query = models.ForeignKey(Query, on_delete = models.CASCADE, default = -1, null = True) # Reference to Query
    tables = models.CharField(max_length = 1024, default = '')                                # Array of table names for this task

# Represents some work, where a user has been assigned a query and a table to annotate
class Work(models.Model):
    user = models.ForeignKey(User, on_delete = models.CASCADE, default = -1, null = True)   # Reference to user
    task = models.ForeignKey(Task, on_delete = models.CASCADE, default = -1, null = True)   # Reference to Task

# Work that has already been completed
class CompletedWork(models.Model):
    work = models.ForeignKey(Work, on_delete = models.CASCADE, default = -1, null = True)   # Reference to user
    elapsed_time = models.FloatField(default = -1.0)                                            # Elapsed time in seconds to complete assigned task
