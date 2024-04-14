#-------------------------------------------------------------------------
# AUTHOR: Francis Nguyen
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Functions for driver program
# FOR: CS 4250- Assignment #3
# TIME SPENT: 2 hrs
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import logging
from pymongo import MongoClient
import string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connectDataBase():

    # Create a database connection object using pymongo
    try: 
        client = MongoClient('mongodb://localhost:27017')
        db = client['cs4250']
        logger.info("Successfully connected to MongoDB")
        return db
    except Exception as e: 
        logger.error("Error connecting to MongoDB: %s", str(e))
        raise

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # Create translation table to remove punctuation
    translator = str.maketrans('', '', string.punctuation)

    # Remove punctuation from docText and convert to lowercase
    clean_text = docText.translate(translator).lower()

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    term_count = {}
    for term in clean_text.split():
        term_count[term] = term_count.get(term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    terms_list = []
    for term, count in term_count.items():
        terms_list.append({"term": term, "count": count, "num_char": len(term)})

    # produce a final document as a dictionary including all the required document fields
    document = {
        "docId": docId,
        "docText": docText,
        "docTitle": docTitle,
        "docDate": docDate,
        "docCat": docCat,
        "terms": terms_list
    }

    # insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"docId": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    index = {}
    for document in col.find():
        for term_object in document['terms']:
            term = term_object['term']
            count = term_object['count']
            if term not in index:
                index[term] = document['docTitle'] + ":" + str(count)
            else:
                index[term] += ", " + document['docTitle'] + ":" + str(count)
    return index