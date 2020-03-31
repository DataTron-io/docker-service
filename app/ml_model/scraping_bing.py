from bs4 import BeautifulSoup
import requests


def searchSingle(query):
    try:
        query = str(query)
    except:
        message = "Invalid input, cannot be parsed."
        status_code = 400
        logging.error(message)
    address = "https://www.bing.com/search?q=" + query.replace(" ", "+") 
    try:
        htmlResult = requests.get(address, headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'}).text
    except:
        message = "GET Request failed"
        status_code = 400
       # logging.error(message)
    try:
        soup = BeautifulSoup(htmlResult, 'html.parser')
        [s.extract() for s in soup('span')]
        unwantedTags = ['a', 'strong', 'cite']
        for tag in unwantedTags:
            for match in soup.findAll(tag):
                match.replaceWithChildren()
        results = soup.findAll('li', { "class" : "b_algo" })
        if not results:
            logging.error("No results found")
            return
    except:
        message = "No results found"
        status_code = 204
        logging.error(message)
    cur, num_results = 0, 3
    descriptions = []
    titles = []
    for result in results:
        try:
            title = str(result.find('h2').get_text()).replace(" ", " ")
            desc = str(result.find('p').get_text()).replace(" ", " ")
            descriptions.append(desc)
            titles.append(title)
        except:
            continue   
        cur += 1
        if cur == num_results:
            break
    result = ''.join(descriptions) + ' ' + ''.join(titles) + ' ' + query
    return result
