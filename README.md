CC-Bill-Tracker
===============

These map reduce functions use Common Crawl data to look at the spread of congressional legislation on the internet.

Program Files:

1. BillCounter: counts on how many pages the bill, in any of its forms, has been mentioned
2. DomainAnalysis: records the domains of pages that mention a bill, in any of its forms, and outputs the 50 domains that have mentioned the bill the most (with their count of pages that have mentioned the bill)
3. AssociationAnalysis: outputs the top 50 words found across all pages that mention a bill in any of its forms, less a set of 100 very common words