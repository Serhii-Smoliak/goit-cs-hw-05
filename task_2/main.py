import string
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
import logging


def get_text(url):
    """Get text from the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Failed to get text from {url}: {e}")
        return None


def remove_punctuation(text):
    """Remove punctuation from the given text."""
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word):
    """Map function for counting word occurrences."""
    return word, 1


def shuffle_function(mapped_values):
    """Shuffle function to group values by key."""
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    """Reduce function to sum values by key."""
    key, values = key_values
    return key, sum(values)


def map_reduce(text, search_words=None):
    """Perform the map-reduce operation on the given text."""
    text = remove_punctuation(text)
    words = text.split()
    if search_words:
        words = [word for word in words if word in search_words]
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))
    shuffled_values = shuffle_function(mapped_values)
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))
    return dict(reduced_values)


def visualize_top_words(result, top_n=10):
    """Visualize the top N most frequent words."""
    if not result:
        logging.warning("Result is empty, nothing to visualize.")
        return
    sorted_words = sorted(
        result.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]
    if not sorted_words:
        logging.warning("Not enough words for visualization.")
        return
    words, frequencies = zip(*sorted_words)
    plt.figure(figsize=(10, 8))
    plt.barh(words, frequencies, color='skyblue')
    plt.ylabel('Words')
    plt.xlabel('Frequency')
    plt.title(f'Top {top_n} Most Frequent Words')
    plt.xticks(rotation=45)
    plt.show()


if __name__ == '__main__':
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)
    if text:
        search_words = [
            'war', 'peace', 'love', 'cigarette', 'helplessness',
            'monologue', 'straggled', 'sigh', 'irregular', 'example', 'words',
        ]
        result_data = map_reduce(text, search_words)
        if not result_data:
            logging.error("MapReduce returned an empty result.")
        else:
            print("Word count result:", result_data)
            visualize_top_words(result_data)
    else:
        logging.error("Error: Failed to get input text.")
