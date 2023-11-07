from django import template

register = template.Library()

@register.filter
def index(lst, index):
    return lst[index]

@register.filter
def display(cell):
    return cell.replace('_', ' ')

@register.filter
def is_link(cell):
    if '_' in cell:
        return '1'
    
    return '0'