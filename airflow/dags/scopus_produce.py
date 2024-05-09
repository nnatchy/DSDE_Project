import hashlib
import io
import json
import os
import zipfile
import pandas as pd
import chardet

from kafka import KafkaConsumer, KafkaProducer

import re
from bs4 import BeautifulSoup
import requests

from kafka import KafkaProducer
import time
import json

import pprint  # Import pprint for pretty printing

# function to find how many author are in this paper
def num_author_group(head_data):
  num = 0
  for author in head_data:
    if int(author['seq']) > num:
      num = int(author['seq'])

  return num

def process_head_data(bibrecord_data):
    # process author
    author_groups = bibrecord_data["head"]["author-group"]
    if isinstance(author_groups, dict):
        author_groups = [author_groups]  
    
    head_output_data = {"author_groups": []}

    for author in author_groups:
        affi = author.get("affiliation", {})
        org = affi.get("organization", [])
        if isinstance(org, dict):  
            organization_names = [org["$"]]
        elif isinstance(org, list):  
            organization_names = [o["$"] for o in org]
        else:
            organization_names = []  
    
        affi_info = {
            "affiliation_id": affi.get("@afid", ""),
            "dpt_id": affi.get("@dptid", ""),
            "country": affi.get("country", ""),
            "organization": organization_names  
        }
    
        authors = author.get("author", [])
        if isinstance(authors, dict):  
            authors = [authors]
    
        for person in authors:
            author_info = {
                "indexed-name": person.get("preferred-name", {}).get("ce:indexed-name", ""),
                "seq": person.get("@seq", ""),
                "auid": person.get("@auid", ""),
                "affiliation": affi_info  # Include affiliation info
            }
            head_output_data["author_groups"].append(author_info)
    head_output_data["enhancement"] = []

    # process classifiaction group
    if "enhancement" in bibrecord_data["head"] and "classificationgroup" in bibrecord_data["head"]["enhancement"]:
        classifications = bibrecord_data["head"]["enhancement"]["classificationgroup"].get("classifications", [])

        if isinstance(classifications, dict):
            classifications = [classifications] 
    
        for classification in classifications:
            if classification["@type"] == "SUBJABBR":  
                if isinstance(classification.get("classification"), (list, dict)):
                    if isinstance(classification["classification"], list):
                        all_classifications = [item.get("$", item) if isinstance(item, dict) else item for item in classification["classification"]]
                    else:  
                        all_classifications = [classification["classification"].get("$", classification["classification"])]
                else:  
                    all_classifications = [classification["classification"]]
    
                class_info = {
                    "type": classification["@type"],
                    "classifications": all_classifications
                }
                head_output_data["enhancement"].append(class_info)

    # Extract source information
    head_output_data["source"] = {
        "publication_date": bibrecord_data["head"]["source"]["publicationdate"],
    }

    
    return head_output_data

def get_citation_count(DOI):
    
    API_KEY = "8a4d62cf82cf68382f5f55a658354336"
    BASE_URL = "http://api.elsevier.com/content/search/scopus"
    
    headers = {
        "X-ELS-APIKey": API_KEY
    }
    
    query_url = f"{BASE_URL}?query=DOI({DOI})&field=citedby-count"
    response = requests.get(query_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        cited_by_count = data["search-results"]["entry"][0]["citedby-count"]
        # print(f"The document with DOI {DOI} has been cited by {cited_by_count} times in Scopus.")
    else:
        print("Error:", response.status_code)

    return cited_by_count

def process_tail_data(bibrecord_data):
    # Ensure 'tail' is a dictionary before proceeding
    tail_info = bibrecord_data.get("tail", {})

    bibliography_output = {
        "refcount": "",
        "references": []
    }

    if not tail_info:  # Check if tail_info is empty or None
        return bibliography_output

    bibliography = tail_info.get("bibliography", {})
    bibliography_output["refcount"] = bibliography.get("@refcount", "0")

    # Handle references
    references = bibliography.get("reference", [])
    if not isinstance(references, list):
        references = [references] if references else []

    for ref in references:
        reference_info = {
            "id": ref.get("@id", ""),
            "ref_fulltext": ref.get("ref-fulltext", ""),
            "ref_text": ref.get("ce:source-text", ""),
            "ref_info": {},
            "ref_authors": [],
            "ref_authors_count": "",
            "ref_collab": []
        }

        ref_info = ref.get("ref-info", {})
        reference_info["ref_info"] = {
            "ref_publicationyear": ref_info.get("ref-publicationyear", {}).get("@first", ""),
            "ref_title": ref_info.get("ref-title", {}).get("ref-titletext", "Title Not Available"),
            "ref_sourcetitle": ref_info.get("ref-sourcetitle", "")
        }

        # Process authors and collaborations
        ref_authors = ref_info.get("ref-authors", {})
        authors = ref_authors.get("author", [])
        if not isinstance(authors, list):
            authors = [authors] if authors else []
        reference_info["ref_authors"] = [author.get("ce:indexed-name", "") for author in authors]
        reference_info["ref_authors_count"] = len(reference_info["ref_authors"])

        collaborations = ref_authors.get("collaboration", [])
        if not isinstance(collaborations, list):
            collaborations = [collaborations] if collaborations else []
        reference_info["ref_collab"] = [{"collaboration_name": collab.get("ce:text", "")} for collab in collaborations]

        bibliography_output["references"].append(reference_info)

    return bibliography_output

def get_all_feature(data, file_name):
    output_data = {
        "id": str(file_name),
        "publicDate": "",
        "source": 1,
        "coAuthorship": 0,
        "citationCount": 0,
        "refCount": 0,
        "Class": []
    }

    bibrecord_data = data['item']['bibrecord']
    
    head_data = process_head_data(bibrecord_data)

    # find publication date
    date = head_data['source']['publication_date']
    if 'day' not in date:
        output_data['publicDate'] = None
    else:
        output_data['publicDate'] = date['day'] + '/' + date['month'] + '/' + date['year']

    # find num authorship
    output_data['coAuthorship'] = num_author_group(head_data['author_groups'])

    # find citation count
    item_info = bibrecord_data['item-info']
    if "ce:doi" not in item_info['itemidlist']:
        output_data['citationCount'] = None
    else :
        doi = item_info['itemidlist']['ce:doi']
        output_data['citationCount'] = get_citation_count(doi)

    # find the references count
    tail_data = process_tail_data(bibrecord_data)
    output_data['refCount'] = tail_data['refcount']

    # get the class
    enhancement_data = head_data['enhancement']
    for item in enhancement_data:
        if item['type'] == 'SUBJABBR':
            for subject in item['classifications']:
                output_data['Class'].append(subject)
    
    return output_data

def process_directory(year):
    base_path = "data/scopus"
    producer = KafkaProducer(
        bootstrap_servers=['kafka1:19092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        request_timeout_ms=60000,
        retry_backoff_ms=500,
    )

    year_path = os.path.join(base_path, str(year), f'{year}_test')
    if year == 2018:
        year_path = os.path.join(base_path, str(year), f'{year} copy')
    if os.path.isdir(year_path):
        for file_name in os.listdir(year_path):
            if file_name == '.DS_Store': continue
            file_path = os.path.join(year_path, file_name)
            if os.path.getsize(file_path) == 0:
                print(f"Skipping empty file: {file_path}")
                continue
            try:
                with open(file_path, 'rb') as f:
                    raw_data = f.read()
                    result = chardet.detect(raw_data)
                    encoding = result['encoding']

                with open(file_path, 'r', encoding=encoding) as file:
                    json_data = json.load(file)
                    abstracts_info = json_data.get("abstracts-retrieval-response", {})
                    data = get_all_feature(abstracts_info, file_name)
                    producer.send(f'scopus-topic-{year}', value=data).get(timeout=30)
                    print(f"Sent: {data}")
            except json.JSONDecodeError as e:
                print(f"JSON Decode Error in file {file_path}: {str(e)}")
            except Exception as e:
                print(f"Failed to process file {file_path}: {str(e)}")
            time.sleep(1)
    producer.flush()
