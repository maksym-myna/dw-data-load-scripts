# Книгозбірня

## **Description**

"Книгозбірня" is a web application designed to modernize library operations and promote reading in Ukraine. The website offers a user-friendly way to browse book information, borrow books, rate books, and create reading lists. 

## **Key Features**

*   **Online Book Ordering:** Users can browse the book catalog and place orders online. 
*   **Ratings and Lists:** Users can rate books and add them to "Want to Read," "Currently Reading," and "Already Read" lists. 
*   **Advanced Filtering:** Users can filter books by various attributes, including author, genre, language, and publisher. 
*   **Admin Panel:** Administrators have access to dashboards with analytics, can export data, populate the database with historical data, and run ETL processes. 
*   **Integration with Google OAuth 2.0:** Users can register and authenticate using their Google accounts. 

## How to use:
1. Clone the [back-end repo](https://github.com/maksym-myna/knyhozbirnia) and add your database connection info to application.yml
2. Start the server running `mvn spring-boot:run` in the console or in your preffered IDE.
3. Clone the [front-end repo](https://github.com/maksym-myna/kyhozbirnia-react) *(optionally change back-end url in properties)*
4. Start the application running `npm start` in the console or in your preffered IDE.
5. *[OPTIONAL] Clone the [data population repo](https://github.com/maksym-myna/dw-data-load-scripts) (and add credentials) for database population*
6. Start the python script running `python scripts/data_parser.py` in the console or in your preffered IDE.
7. *[OPTIONAL] Clone the [ETL repo](https://github.com/maksym-myna/etl) (and add credentials) to set up ETL-pipeline to BigQuery*
8. Start the python script running `python etl.py` in the console or in your preffered IDE.

## **Database Schema**
![EER](https://i.ibb.co/f8862GG/eer-1.png)

## **Business Processes**

*   Authorization
*   Reading Tracking
*   Loan Processing
*   Entity Management by Administrator

## **Interface**
* Start Page
![](https://i.ibb.co/Qv5nK48/main.png)

* Books Page
![](https://i.ibb.co/ZKSJY9N/filtering.png)

* Genres Page
![](https://i.ibb.co/X5Sbqtd/genres.png)

* Listing
![](https://i.ibb.co/59xG4zq/listing.png)
![](https://i.ibb.co/f1hdBd3/listing-filter.png)

* Rating
![](https://i.ibb.co/L8Qjpt5/rating.png)
![](https://i.ibb.co/3YDtVsX/rating-filter.png)

* Loan
![](https://i.ibb.co/smyRfMV/loan.png)


* Loaned books (admin view)
![](https://i.ibb.co/C9d9LcB/profile.png)

* Loaned books (user view)
![](https://i.ibb.co/r0rhQxr/loans-from-user.png)

* Loans
![](https://i.ibb.co/Sw8y7RD/loans.png)
* Loan Return
![](https://i.ibb.co/PxHpzhm/loan-by-administrator.png)



* Work addition
![](https://i.ibb.co/rsW5hKG/add-work.png)
![](https://i.ibb.co/jLW4Qyh/added-work.png)

* Work edit
![](https://i.ibb.co/fXt3C9k/work-edit.png)
![](https://i.ibb.co/3rxbD7N/edited-work.png)

* Dashboards
![](https://i.ibb.co/4ssx36V/dashboard-1.png)
![](https://i.ibb.co/QpvnZyP/dashboard-2.png)
![](https://i.ibb.co/CWYhqpS/dashboard-3.png)

* Export & Import 
![](https://i.ibb.co/PW7ZNBm/import-export.png)

## **Tech Stack**

### **Database**

*   Google Cloud SQL (PostgreSQL)
*   React + TypeScript
*   Spring Boot
*   Python
*   SQLite
*   Google Cloud Storage
*   Apache Beam
*   BigQuery
*   Looker Studio
