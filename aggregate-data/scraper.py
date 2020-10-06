#!/Library/Frameworks/Python.framework/Versions/3.7/bin/python3
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import time
import pandas as pd
import multiprocessing as mp


# # starting from the bioinformatics biorxiv
# url = 'https://www.biorxiv.org/collection/bioinformatics'
# driver.get(url)


def get_single_article(url):
    print(url)
    driver2 = webdriver.Chrome(executable_path='/Users/michael/Downloads/chromedriver79')
    driver2.get(url)
    date = None
    # get the date
    try:
        pane = driver2.find_elements_by_class_name('pane-1')
    except NoSuchElementException:
        print('No Date')
        with open("url-errors.txt", "a") as file_object:
            file_object.write(url+'\n')
        return 'No Date', 'No Date'

    if len(pane) == 1:
        date = pane[0].text
        date = date.replace('Posted ', '')
        date = date.replace('.', '')
    else:
        print('Fail on URL: ' + url)
        driver2.quit()
        return 'Fail', 'Fail'

    # get the abstract
    try:
        abstract = driver2.find_element_by_id('abstract-1')
    except NoSuchElementException:
        print('Abstract Not Found: ' + url)
        with open("url-errors.txt", "a") as file_object:
            file_object.write(url+'\n')
        driver2.quit()
        return date, "No Abstract"

    abstract = abstract.text
    abstract = abstract.replace('\n',' ')
    abstract = abstract.replace('\\n', ' ')
    abstract = abstract.replace('\\\n', ' ')
    driver2.quit()
    return date, abstract


def extract_article_list(driver):
    print('Getting Article List')
    # get all the articles links on this page
    links = []
    dates = []
    abstracts = []
    a_tags = driver.find_elements_by_class_name('highwire-cite-linked-title')
    for a in a_tags:
        links.append(a.get_attribute('href'))
        # get the information about the individual article, the date and abstract
        date, ab = get_single_article(links[-1])
        dates.append(date)
        abstracts.append(ab)

    # get all the titles, there are duplicates of each title so skip every other
    odd = False
    titles = []
    for span in driver.find_elements_by_class_name('highwire-cite-title'):
        if odd:
            titles.append(span.text)
            odd = False
        else:
            odd = True

    # get authors
    authors = []
    for span in driver.find_elements_by_class_name('highwire-citation-authors'):
        authors.append(span.text)

    print('Writing Data')
    try:
        data = pd.DataFrame(
            {'Date': dates, 'Title': titles, 'Authors': authors, 'Article_link': links, 'Abstract': abstracts})
    except ValueError:
        print('The lists are probably not the same legnths in the above dataframe: ' + str(driver.current_url))
        with open("url-errors.txt", "a") as file_object:
            file_object.write(driver.current_url+'\n')
        return
    data.to_csv('biorxiv.csv', mode='a', header=False)
    print('Done Writing')


def scrape_rxiv(start, end):
    base_url = 'https://www.biorxiv.org/collection/bioinformatics?page='
    page_index = start
    # using chrome instead of FireFox because biorxiv does not load on firefox
    driver = webdriver.Chrome(executable_path='/Users/michael/Downloads/chromedriver79')
    while True and page_index <= end:
        print('Page ' + str(page_index))
        driver.get(base_url + str(page_index))
        start = time.time()
        extract_article_list(driver)
        print('Time For Page ' + str(page_index) + ': ' + str(start - time.time()))
        page_index += 1
        # check if there is a next button, if there is not, break out of loop
        if len(driver.find_elements_by_class_name('link-icon-after')) == 0:
            break
    # close the browser
    driver.quit()


def start_func(params):
    print(params)
    scrape_rxiv(int(params[0]), int(params[1]))
    return "Done" + str(params[0]) + " " + str(params[1])


def run_in_parallel(param_lists=None):
    # get the number of pages that need to be run
    driver = webdriver.Chrome(executable_path='/Users/michael/Downloads/chromedriver79')
    driver.get('https://www.biorxiv.org/collection/bioinformatics')
    nums = driver.find_elements_by_class_name('last')
    end = int(nums[4].text)
    driver.quit()
    if param_lists is None:
        param_lists = [[i, i + 99] if i + 99 < end else [i, end] for i in range(0, end, 100)]
    print(param_lists)
    pool = mp.Pool(processes=12)
    pool.map(start_func, param_lists)


if __name__ == '__main__':
    # temp_params = [[696, 699], [313, 399], [266, 299]]
    run_in_parallel(temp_params)
