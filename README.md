### Web Service Recommendation
Recommendation systems have been widely used by companies to improve customer interaction and increase the time spent on their platform. And with the recent surge in web services on the internet it has become essential to develop an effective recommendation system which would further improve the customer engagement score. Traditional web service recommendation systems have made use of neighborhood based collaborative filtering with Quality of Service(QoS) as a parameter to predict customer preferences. To further improve the performance and overcome the problems faced by the neighborhood based collaborative filtering algorithm the paper [1] suggests to consider time information as an important parameter along with QoS. In this paper we try to understand the approach mentioned in the above paper and implement the algorithm and revalidate the test results.

#### Contributors
- Adya Shrivastava (as4003@rit.edu)
- Byreddy Vishnu (vb8391@rit.edu)
- Pranjal Pandey (pp9034@rit.edu)
- Rishabh Manish Sahlot (rs3655@rit.edu)

#### Dataset:
In this approach, we use the [WS-dream dataset](https://github.com/wsdream/wsdream-dataset) provided by [2]. It is a publicly available data which was collected in 2011. The WS-Dream dataset is divided into two datasets, the first dataset contains the response time and throughput of the record of service invocation of 339 users and 585 web services. The second dataset contains contains QoS measurements from 142 users on 4,500 Web services. It is taken over 64 consecutive time slices which is at 15 min intervals. The preprocessing involves removing duplicates from response time and throughput information.

#### Requirements
- Python 3.7.3+
- Numpy
- PostgreSQL
- psycopg2
- pandas
- tqdm
- pickle
- matplotlib
- mcdm
- Flask

Run `pip install -r requirements.txt` to install all the *Python* dependencies

#### Steps to run the program:
##### Data Loading:
- Create a database on the PostgreSQL server with the name `webservicerecommendation`.
- cd ~/WebServiceRec/CreateDB
- Replace the username and password with PostgreSQL username and password.
- Run `python3 main.py`

##### Web Service Recommendation:
- cd ~/WebServiceRec
- Replace the username and password with PostgreSQL username and password.
- Change the db name to `webservicerecommendation`
- Run `python3 main.py`
- Enter User Location and Service Category and User Id

Ex: User Location=United States,
  Service category=Microsoft,
  User Id = 1

#### References:
[1] Y. Hu, Q. Peng, X. Hu, and R. Yang. Time aware anddata sparsity tolerant web service recommendationbased on improved collaborative filtering.IEEETransactions on Services Computing, 8(5):782–794,2015

[2] Z. Zheng, Y. Zhang, and M. R. Lyu. Investigating qosof real-world web services.IEEE Transactions onServices Computing, 7(1):32–39, 2014
