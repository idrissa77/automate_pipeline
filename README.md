**Title**: 

Helping Users Choose Food Products More Effectively Through a Mobile Application.

This project was carried out during my internship at Match Point (Startup).


**Project Summary**

I focused on a concrete need: helping consumers quickly identify the right product on a supermarket shelf based on their criteria (health, allergies, vegetarian options, palm oil, price). I stored data for roughly 190,000 products in MongoDB through an ETL pipeline. 

The computer vision engineer extracted text from a photo of the shelf. Using this extracted text, I mapped it to the stored products, computed a score (nutri-score, price), and applied the user’s selected filters. APIs handled communication between the vision module, the backend API, and the frontend.


**Result:** scanning a shelf photo trigger the entire process, which runs in a few seconds and displays highlighted products with their information, color codes, and alerts. I also automated product additions and updates with GitHub Actions, scheduled weekly.
