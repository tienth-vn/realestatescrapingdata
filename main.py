import sqlalchemy
from bs4 import BeautifulSoup
import time
import pandas as pd
import os
from sqlalchemy import create_engine
import pyodbc
import requests as rq
from selenium import webdriver
from datetime import datetime
import urllib
from sqlalchemy.types import NVARCHAR
import random
from azure.storage.blob import BlockBlobService, PublicAccess
from urllib.request import urlopen
from shutil import copyfileobj

#connect DB
SQLALCHEMY_DATABASE_URI = 'mssql+pyodbc://username:password@servername.database.windows.net/databasename?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(SQLALCHEMY_DATABASE_URI, fast_executemany=True)

#can ho chung cu - BAN
productId = []
productavatarImage = []
postTitle = []
price = []
area = []
location = []
bedroom = []
toilet = []
postDescription = []
dateSubmitted = []
contactName = []
contactMobile = []
getDate = []
pageUrl = []
category = []

# can ho chung cu - BAN
path = "../Real Estate Scraping Data/chromedriver.exe"
driver = webdriver.Chrome(path)
#change the path for each portfolio of real estate
url = "https://xxxx.com/ban-can-ho-chung-cu"
time.sleep(5)

# "Product Id" list existing database
lastedproduct = pd.read_sql_query('''SELECT distinct [Product Id] FROM SCHEMA.Tablename''',con=engine)
lastedproduct['Product Id'] = lastedproduct['Product Id'].astype(str)

# get data containing real estate broker contact
scenarioHtml = ['pr-container vip0 vipaddon product-item clearfix badReportItem', 'pr-container vip0 product-item clearfix badReportItem', 'pr-container vip1 vipaddon product-item clearfix badReportItem', 'pr-container vip1 product-item clearfix badReportItem'
    , 'pr-container vip3 vipaddon product-item clearfix badReportItem', 'pr-container vip3 product-item clearfix badReportItem', 'pr-container vip4 vipaddon product-item clearfix badReportItem', 'pr-container vip4 product-item clearfix badReportItem'
    , 'pr-container vip5 vipaddon product-item clearfix badReportItem', 'pr-container vip5 product-item clearfix badReportItem']

for pagenumber in range(1,7000):
    driver.get(url+"/p"+str(pagenumber))
    timerandom = random.randrange(10,15)
    time.sleep(timerandom)
    content = driver.page_source
    soup = BeautifulSoup(content)
    for i,d in enumerate(soup.find_all("div",{"class":scenarioHtml})):
        productId.append(d.get('prid').strip())
        productimageValue = soup.find_all("img",{"class":"product-avatar-img"})[i].attrs['data-listing'].split(",")
        productavatarImage.append(productimageValue if productimageValue else "" )
        location.append(d.find('span',{"class","location"}).text.strip())

        postTitleValue = d.find('span',{"class","pr-title vipFive product-link"}) #change to "VipZero" when get pagenumber 1-60, "VipTwo" when get pagenumber 61-261, "VipFour" from 262 onwards 
        if postTitleValue is None:
            postTitleValue = d.find('span',{"class","pr-title vipFour product-link"}) #change to "VipOne" when get pagenumber 1-60, "VipThree" when get pagenumber 61-261, "VipFive" from 262 onwards

            if postTitleValue is None:
                postTitle.append(None)
            else:
                postTitle.append(postTitleValue.text.strip())
        else:
            postTitle.append(postTitleValue.text.strip())

        price.append(d.find('span',{"class","price"}).text.strip())
        areaValue = d.find('span',{"class","area"})
        area.append(areaValue.text.strip() if areaValue else "Không xác định")
        bedroomValue = d.find("span",{"class","bedroom"})
        bedroom.append(bedroomValue.text.strip() if bedroomValue else 0 )
        toiletValue = d.find("span",{"class","toilet"})
        toilet.append(toiletValue.text.strip() if toiletValue else 0)
        postDescription.append(d.find("div",{"class","product-content"}).text.strip())
        dateSubmitted.append(d.find("span",{"class","tooltip-time"}).text.strip())
        contactnameValue = d.find("span",{"class","contact-name"})
        contactName.append(contactnameValue.text.strip() if contactnameValue else None)
        contactmobileValue = d.find("span",{"class","hidden-phone contact-phone btn-blue-7"})
        contactMobile.append(contactmobileValue.get('mobile').strip() if contactmobileValue else None )
        getDate.append(datetime.today().strftime('%Y-%m-%d'))
        pageUrl.append("batdongsan.com.vn")
        category.append("Căn hộ chung cư - Bán")

time.sleep(20)
data = pd.DataFrame({'Product Id': productId,
                     'Image url': productavatarImage,
                     'Address': location,
                     'Title': postTitle,
                     'Price': price,
                     'Area': area,
                     'Bedroom': bedroom,
                     'Toilet': toilet,
                     'Description': postDescription,
                     'Updated Time': dateSubmitted,
                     'Contact Name': contactName,
                     'Contact Mobile': contactMobile,
                     'Get Date': getDate,
                     'Page': pageUrl,
                     'Category': category
                     })

time.sleep(20)
data = data[~data['Product Id'].isin(lastedproduct['Product Id'])]

#drop duplicate product id
data.drop_duplicates(subset ="Product Id",
                     keep = 'first', inplace = True)

#explode list contain image from post
data_ex = data.explode("Image url")


def upload_image_blob(img):
    try:
        # Create the BlockBlobService that is used to call the Blob service for the storage account
        blob_service_client = BlockBlobService(
            account_name='realestatedata', account_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

        # Container Name in Storage
        container_name = 'image'

        # Set the permission so the blobs are public.
        blob_service_client.set_container_acl(container_name, public_access=PublicAccess.Container)

        # Create Sample folder if it not exists, and create a file in folder Sample to test the upload and download.
        local_path = os.path.abspath(os.getcwd())
        local_file_name = img
        full_path_to_file = os.path.join(local_path, local_file_name)

        # Upload the created file, use local_file_name for the blob name
        blob_service_client.create_blob_from_path(
            container_name, local_file_name, full_path_to_file)
        os.remove(full_path_to_file)
    except Exception as e:
        print(e)

# download to local file and upload to blob storage
# change image url to blob storage url
for i, d in enumerate(data_ex['Image url']):
    try:
        with urlopen(d) as in_stream, open(d.split('/')[-1], 'wb') as out_file:
            copyfileobj(in_stream, out_file)

        upload_image_blob(d.split("/")[-1])
        rp = d.replace(d,'https://realestatedata.blob.core.windows.net/image/' + d.split("/")[-1])
        data_ex['Image url'].iloc[i] = rp
    except:
        rp = d.replace(d,'')
        data_ex['Image url'].iloc[i] = rp
        continue

#convert to datetime for column
data_ex['Get Date'] = pd.to_datetime(data_ex['Get Date'])

#split column and format
data_ex['Price Unit'] = data_ex.apply(lambda x: "Giá thỏa thuận" if x['Price'] == "Giá thỏa thuận" else x['Price'].split(' ')[1], axis=1)
data_ex['Price'] = data_ex.apply(lambda x: 0 if x['Price'] == "Giá thỏa thuận" else x['Price'].split(' ')[0], axis=1)
data_ex['Area Unit'] = data_ex.apply(lambda x: "Không xác định" if x['Area'] == "Không xác định" else x['Area'].split(' ')[1], axis=1)
data_ex['Area'] = data_ex.apply(lambda x: 0 if x['Area'] == "Không xác định" else x['Area'].split(' ')[0], axis=1)

#import final data to sql server
data_ex.to_sql('Tablename', con=engine, index=False, if_exists="append", schema='Schemaname',
               dtype={'Product Id' : sqlalchemy.types.INTEGER(),
                      'Image url': sqlalchemy.types.VARCHAR(),
                      'Address': sqlalchemy.types.NVARCHAR(),
                      'Title': sqlalchemy.types.NVARCHAR(),
                      'Bedroom':  sqlalchemy.types.VARCHAR(),
                      'Toilet':  sqlalchemy.types.VARCHAR(),
                      'Description': sqlalchemy.types.NVARCHAR(),
                      'Contact Name': sqlalchemy.types.NVARCHAR(),
                      'Contact Mobile':  sqlalchemy.types.NVARCHAR(),
                      'Page': sqlalchemy.types.NVARCHAR(),
                      'Category': sqlalchemy.types.NVARCHAR(),
                      'Price Unit': sqlalchemy.types.NVARCHAR(),
                      'Area Unit': sqlalchemy.types.NVARCHAR()
                      })

driver.close()

## List images stored in existing blob storage

# from azure.storage.blob import BlockBlobService, PublicAccess
# blob_service_client = BlockBlobService(account_name='realestatedata',
#                                        account_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
#
# container_name = 'image'
# listblob=[]
# # Set the permission so the blobs are public.
# blob_service_client.set_container_acl(
#     container_name, public_access=PublicAccess.Container)
# print("\nList blobs in the container")
# generator = blob_service_client.list_blobs(container_name)
# for blob in generator:
#     listblob.append(blob.name)