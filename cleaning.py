import re

def clean_text(text):
    """
    Function to clean text
    """
    text = text.lower()
    text = re.sub("[^a-z]", ' ', text)
    text = text.split()
    return text

def clean_type(text):
    text = re.sub("[^a-z]", "", text)
    text = text.lower()
    pattern_dict = {'cr': 'credit', 'dr': 'debit', 'charged': 'debit',
                     'debited': 'debit','creditedit': 'credit', 'debitdebit':'debit'}
    if text in list(pattern_dict.keys()):
        clean_text = re.sub(text, pattern_dict[text], text)
        return clean_text
    else:
        return text

def get_search_group(text):
    search = re.search("De(.*)|Cre(.*)", text,re.IGNORECASE)
    if search:
        return search.group()
