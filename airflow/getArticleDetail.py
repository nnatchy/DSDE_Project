import json
import requests
from bs4 import BeautifulSoup

import os
import json

# Function to get HTML content of a link
def get_html_content(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch {url}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred while fetching {url}: {e}")
        return None

# Function to extract specific information from HTML content
def extract_info(html_content, article_link, article_name=None):
    soup = BeautifulSoup(html_content, 'html.parser')
    header_info = soup.find_all('header')
    if len(header_info) >= 2:
        header_tag = header_info[1]  # Get the second header tag
        
        # Extract title, article category, published date, authors list, journal information, accesses, and altmetric
        article_title_tag = header_tag.find('h1', class_='c-article-title')
        article_title = article_title_tag.get_text(strip=True) if article_title_tag else "Title not found"
        
        article_category_tag = header_tag.find('li', {'data-test': 'article-category'}, class_='c-article-identifiers__item')
        article_category = article_category_tag.get_text(strip=True) if article_category_tag else "Category not found"
        
        published_date_tag = header_tag.find('li', class_='c-article-identifiers__item').find('time')
        published_date = published_date_tag.string.strip() if published_date_tag else "N/A"
        
        authors_list = [author.get_text(strip=True) for author in header_tag.find_all('a', {'data-test': 'author-name'})]
        
        journal_link_tag = header_tag.find('a', {'data-test': 'journal-link'})
        journal_link = journal_link_tag.get_text(strip=True) if journal_link_tag else "Journal link not found"
        
        journal_volume_tag = header_tag.find('b', {'data-test': 'journal-volume'})
        journal_volume = journal_volume_tag.get_text(strip=True) if journal_volume_tag else "Volume not found"
        journal_volume = journal_volume[6::]
        
        article_number_tag = header_tag.find('span', {'data-test': 'article-number'})
        article_number = article_number_tag.get_text(strip=True) if article_number_tag else "Article number not found"
        
        article_publication_year_tag = header_tag.find('span', {'data-test': 'article-publication-year'})
        article_publication_year = article_publication_year_tag.get_text(strip=True) if article_publication_year_tag else "Publication year not found"
        
        accesses_tag = header_tag.find('p', class_='c-article-metrics-bar__count')
        accesses_text = accesses_tag.get_text(strip=True, separator=' ')
        accesses = accesses_text.split()[0] if accesses_text else "Accesses not found"

        altmetric_tag = header_tag.find_all('p', class_='c-article-metrics-bar__count')
        altmetric_text = altmetric_tag[1].get_text(strip=True, separator=' ') if len(altmetric_tag) >= 2 else ""
        altmetric = altmetric_text.split()[0] if altmetric_text else "Altmetric not found"

        # Extract abstract
        abstract_section = soup.find('section', {'aria-labelledby': 'Abs1'})
        if abstract_section:
            abstract_content = abstract_section.find('div', class_='c-article-section__content')
            abstract = abstract_content.get_text(strip=True) if abstract_content else "Abstract not found"
        else:
            abstract = "Abstract section not found"

        # Extract references section
        references_section = soup.find('section', {'aria-labelledby': 'Bib1'})
        if references_section:
            references_items = references_section.find_all('li', class_='c-article-references__item')
            references = []

            for reference_item in references_items:
                reference_text = reference_item.find('p', class_='c-article-references__text')
                reference_details = reference_text.get_text(strip=True)

                article_link = None
                cas_link = None
                google_scholar_link = None

                reference_links = reference_item.find_all('a')

                for link in reference_links:
                    if 'Article' in link.get_text():
                        article_link = link['href']
                    elif 'CAS' in link.get_text():
                        cas_link = link['href']
                    elif 'Google Scholar' in link.get_text():
                        google_scholar_link = link['href']

                references.append({
                    "Details": reference_details,
                    "Article Link": article_link,
                    "CAS Link": cas_link,
                    "Google Scholar Link": google_scholar_link
                })
        else:
            references = "References section not found"



        # Initialize a list to store recommendations
        recommendations = []

        # Extract similar content
        similar_content_section = soup.find('section', {'aria-labelledby': 'inline-recommendations'})
        if similar_content_section:
            article_items = similar_content_section.find_all('article', class_='c-article-recommendations-card')
            
            for article_item in article_items:
                title_tag = article_item.find('h3', class_='c-article-recommendations-card__heading')
                title = title_tag.text.strip() if title_tag else "Title not found"
                
                link_tag = article_item.find('a', class_='c-article-recommendations-card__link')
                link = link_tag['href'] if link_tag else "Link not found"
                
                # Append recommendation to the list
                recommendations.append({"Title": title, "Link": link})

        else:
            print("Similar content section not found")

        # Extract published date and DOI
        published_date = None

        bibliographic_items = soup.find_all('li', class_='c-bibliographic-information__list-item')
        for item in bibliographic_items:
            if 'Published' in item.text:
                published_date_tag = item.find('time')
                if published_date_tag:
                    published_date = published_date_tag['datetime']

        # Extract further reading section
        further_reading_section = soup.find('div', {'id': 'further-reading-section'})
        if further_reading_section:
            further_reading_items = further_reading_section.find_all('li', class_='c-article-further-reading__item')

            cited_by_articles = []

            for item in further_reading_items:
                title_tag = item.find('h3', class_='c-article-further-reading__title')
                title = title_tag.text.strip() if title_tag else "Title not found"
                link = title_tag.find('a')['href'] if title_tag else None

                cited_authors_list_tag = item.find('ul', class_='c-author-list')
                cited_authors_list = [author.text.strip() for author in cited_authors_list_tag.find_all('li')] if cited_authors_list_tag else []

                journal_title_tag = item.find('p', class_='c-article-further-reading__journal-title')
                journal_title = journal_title_tag.text.strip() if journal_title_tag else "Journal title not found"

                cited_by_articles.append({
                    "Title": title,
                    "Link": link,
                    "Authors": cited_authors_list,
                    "Journal Title": journal_title
                })
        else:
            cited_by_articles = "Further reading section not found"

        # Create dictionary with extracted information
        article_info = {
            "Article Name": article_name if article_name else "Name not found",
            "Article Link": article_link,
            "Title": article_title,
            "Article Category": article_category,
            "Published Date": published_date,
            "Authors List": authors_list,
            "Journal Link": journal_link,
            "Journal Volume": journal_volume,
            "Article Number": article_number,
            "Published Date": published_date,
            "Publication Year": article_publication_year,
            "Accesses": accesses,
            "Altmetric": altmetric,
            "Abstract": abstract,
            "References": references,
            "Cited": cited_by_articles,
            "Similar Content Recommendations": recommendations
        }
        
        # Return the dictionary
        return article_info

    else:
        print("Second header information not found.")
        return None


def process_article_details():
    # Define the directory paths
    input_directory = "./scrape_nature_json"
    output_directory = "./data"
    for filename in os.listdir(input_directory):
        if filename.endswith(".json"):
            input_file_path = os.path.join(input_directory, filename)
            output_file_path = os.path.join(output_directory, f"data_{filename}")
            with open(input_file_path, 'r', encoding='utf-8') as file:
                articles = json.load(file)
            extracted_info_list = []
            for article in articles:
                html_content = get_html_content(article['Link'])
                if html_content:
                    extracted_info = extract_info(html_content, article['Link'], article.get('Title'))
                    article_with_detail = {**article, "Detail": extracted_info}
                    extracted_info_list.append(article_with_detail)
            with open(output_file_path, 'w') as outfile:
                json.dump(extracted_info_list, outfile, indent=4)
            print(f"Extraction completed for '{filename}'. Results saved to '{output_file_path}'.")

# Call process_article_details() if running as a script (Optional)
if __name__ == "__main__":
    process_article_details()
    