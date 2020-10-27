# Pulse
This project aims to track the trend of word usage in [bioRxiv](https://www.biorxiv.org/) over time.
Search for a term and view a plot of it's occurance over time - essentially the pulse of this the term. Try it out [here](https://msbradshaw.github.io/Pulse/)!

Pulse is a pet project of mine I built at the beginning of quarantine. Long term I'd like Pulse to be more than just a search and plot tool; I envision Pulse as a resource for understanding the current state of life science research from a data and figure based perspective. I hope Pulse will someday be a portal for the science of life sciences.

## Want to contribute?
Here are some features that could be added:
1. The API the front-end queries has support for searching only within a certain date range. The GUI does not currently make use of this feature, feel free to add it!
2. bioRxiv as a whole is becoming more populare, more papers are being published in it every month, to make Pulse's plots meaning full this overall growth needs to be controlled for, otherwise any given search term will look like it is becoming exponentially more populare.
3. medRxiv is set up the exact same as bioRxiv and contains much of the same information, scraping that information and including it in the database would be another cool addition
4. Include pubmed abstracts in the Pulse database. This will cause some redundancy in data since preprints tend to turn into real papers listed on pubmed, but will expand the the size of the database tremendously.
