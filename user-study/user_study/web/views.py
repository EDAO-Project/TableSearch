from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
import json

def read_params(filename):
    with open(filename, 'r') as file:
        return json.load(file)

def login(request):
    template = loader.get_template('login.html')
    return HttpResponse(template.render())

def annotate(request):
    if request.method == 'GET':
        template = loader.get_template('annotate.html')
        return HttpResponse(template.render())
    
    username = request.POST['uname']
    #params = read_params('/user_study/params.json')
    # TODO: Find IDs of queries this user has annotated
    # TODO: Select queries that the user has not annotated yet
    # TODO: Let the user iterate queries and its set of tables one by one, and save the scores of all the tables before moving on to the next query.
    
    template = loader.get_template('annotate.html')
    context = {
        'username': username
    }
    return HttpResponse(template.render(context, request))