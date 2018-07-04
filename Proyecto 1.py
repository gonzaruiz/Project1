
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


df = pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
df.head(2)


# In[3]:


df1 = pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
df1.head(2)


# In[4]:


#Get the Metadata from the above files

df.info()
df1.info()


# In[5]:


list(df.index)


# In[6]:


list(df1.index)


# In[7]:


#Get the row names from the above files

df.index.values


# In[8]:


df1.index.values


# In[9]:


df.columns


# In[10]:


#Rename the columns name from any of the above file.

df.rename(columns={'Indicator':'indicator_id'})


# In[11]:


df1.columns


# In[12]:


df1.rename(columns={'STATION':'GONZALO'})


# In[13]:


df1.columns


# In[14]:


df1.rename(columns={'STATION':'GONZALO'}, inplace=True)


# In[15]:


df1.columns


# In[16]:


df.columns


# In[17]:


df.rename(columns={'Indicator':'indicator_id'}, inplace=True)


# In[18]:


df.columns


# In[19]:


#Change the name of multiple columns

df.rename(columns={'indicator_id':'indicator_gonza','Year':'year_gonza'})


# In[20]:


df1.rename(columns={'GONZALO':'Gonzalo_ID','DATE':'date_gonza'})


# In[21]:


#Arrange values of a particular column in ascending order

df1.sort_values('DATE')


# In[22]:


df.sort_values('Year')


# In[23]:


#Arrange multiple column values in ascending order

df.sort_values(['Country','Year'])


# In[24]:


#Make country as the first column of the dataframe

df.sort_values(['Country','Year'])


# In[25]:




df[['Country', 'indicator_id', 'PUBLISH STATES', 'Year', 'WHO region',
       'World Bank income group', 'Sex', 'Display Value',
       'Numeric', 'Low', 'High', 'Comments']]


# In[26]:


df1[['GONZALO', 'DATE', 'STATION_NAME', 'PRCP', 'SNWD', 'SNOW', 'TMAX',
       'TMIN', 'WDFG', 'PGTM', 'WSFG', 'WT09', 'WT07', 'WT01', 'WT06', 'WT05',
       'WT04', 'WT16', 'WT08', 'WT18', 'WT03']]


# In[27]:


#Get the Column array using a variable

nombre = "Year"
df[nombre].values


# In[28]:


#Get the subset rows 11,24,37

df.loc[[11,24,37]]


# In[29]:


#Get the subset rows excluding 5, 12, 23, 56

lista_indices = list(range(0,5)) + list(range(6,12)) + list(range(13,23)) + list(range(24,56)) + list(range(57, 4656))
df.loc[lista_indices]


# In[30]:


df_transaccion = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')
df_transaccion.head(2)


# In[31]:


df_product = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
df_product.head(2)


# In[32]:


df_session = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
df_session.head(2)


# In[33]:


df_users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
df_users.head(2)


# In[34]:


from pandas import DataFrame, Series
import sqlite3 as db
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())


# In[35]:


pysqldf


# In[36]:


pysqldf("SELECT * FROM df_transaccion;")


# In[37]:


#join users to transactions, keeping all rows from transactions and only matching rows from users (left join)

pysqldf("SELECT * FROM df_transaccion LEFT JOIN df_users ON df_users.UserID = df_transaccion.UserID LIMIT 10;")


# In[38]:


#Which transactions have a UserID not in users?

pysqldf("SELECT df_transaccion.* FROM df_transaccion LEFT JOIN df_users ON df_users.UserID = df_transaccion.UserID WHERE df_users.UserID IS NULL;")


# In[39]:


# 

pysqldf("SELECT * FROM df_transaccion INNER JOIN df_users ON df_users.UserID = df_transaccion.UserID GROUP BY df_users.User;")


# In[40]:


pysqldf("SELECT * FROM df_transaccion INNER JOIN df_users ON df_users.UserID = df_transaccion.UserID ORDER BY df_users.User;")


# In[41]:


pysqldf("""SELECT * FROM df_transaccion LEFT JOIN df_users ON df_users.UserID = df_transaccion.UserID
        UNION
        SELECT df_transaccion.* , df_users.* FROM df_users LEFT JOIN df_transaccion ON df_users.UserID = df_transaccion.UserID;""") 


# In[42]:


#Join users to transactions, displaying all matching rows AND all non-matching rows(full outer join)

pysqldf("""SELECT * FROM df_transaccion LEFT JOIN df_users ON df_users.UserID = df_transaccion.UserID
        UNION
        SELECT df_transaccion.* , df_users.* FROM df_users LEFT JOIN df_transaccion ON df_users.UserID = df_transaccion.UserID
        ORDER BY df_users.User;""") 


# In[43]:


#Determine which sessions occurred on the same day each user registered

pysqldf("SELECT * FROM df_users INNER JOIN df_session ON df_users.Registered = df_session.SessionDate WHERE df_users.UserID = df_session.UserID;")


# In[44]:


#build a database with every possible (userID, productID) pair (cross join)

pysqldf("SELECT df_users.UserID, df_product.ProductID FROM df_users CROSS JOIN df_product LIMIT 13;" )


# In[45]:


# Determine how much quantity of each product was purchased by each user


pysqldf("""SELECT df_users.UserID, df_product.ProductID, SUM(case when typeof(df_transaccion.Quantity) = "integer" then df_transaccion.Quantity else 0 end) AS Quantity
        FROM df_users CROSS JOIN df_product 
        LEFT JOIN df_transaccion ON df_users.UserID = df_transaccion.UserID AND df_product.ProductID = df_transaccion.ProductID
        GROUP BY df_users.UserID, df_product.ProductID;""")


# In[46]:


#For each user, get each possible pair of pair transactions (transactionID1, TransacationID2)


# cambiar nombres de cabecera, usar aliasing

pysqldf("""SELECT df_transaccion_x.TransactionID as TransactionID_x ,
                df_transaccion_x.TransactionDate as TransactionDate_x , *
            FROM df_transaccion as df_transaccion_x  
            CROSS JOIN df_transaccion ON df_transaccion_x.UserID = df_transaccion.UserID;""")


# In[47]:


# Join each user to his/her firt ocurring transaction in the transactions table.

pysqldf("""SELECT df_users.UserID, User, Gender, Registered, Cancelled, TransactionID, MIN(TransactionDate) AS TransactionDate, ProductID, Quantity
            FROM df_users 
            LEFT JOIN df_transaccion ON df_users.UserID = df_transaccion.UserID
            GROUP BY df_users.UserID;""")


# In[48]:


#Test to see if we can drop columns


data = pysqldf("""SELECT df_users.UserID, User, Gender, Registered, Cancelled, TransactionID, MIN(TransactionDate) AS TransactionDate, ProductID, Quantity
            FROM df_users 
            LEFT JOIN df_transaccion ON df_users.UserID = df_transaccion.UserID
            GROUP BY df_users.UserID;""")

my_columns = list(data.columns)
my_columns

list(data.dropna(thresh=int(data.shape[0]*.9),axis=1).columns)

missing_info = list(data.columns[data.isnull().any()])
missing_info

for col in missing_info:
    num_missing = data[data[col].isnull() == True].shape[0]
    print('number missing for column{}:{}'.format(col, num_missing))
    
print('-------------------------------------------')
    
for col in missing_info:
    percent_missing = data[data[col].isnull() == True].shape[0] / data.shape[0]
    print('percent missing for column{}:{}'.format(col, percent_missing))
    

