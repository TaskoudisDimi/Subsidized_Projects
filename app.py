#Import libraries
from flask import Flask, render_template, request, redirect
from flask_mysqldb import MySQL
import numpy as np
from pandas import DataFrame
import statistics as st
from statistics import mode
from forms import ContactForm
import pandas as pd



app = Flask(__name__)


app.secret_key = 'My app'

# Define details for connecting with db
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = '7741'
app.config['MYSQL_DB'] = 'despw_thesis'


mysql = MySQL(app)


# endpoint for Home page
@app.route('/')
def home():
    return render_template('Home_1.html')

# endpoint for Home page
@app.route('/home')
def Home():
    return render_template('Home_1.html')



from werkzeug.exceptions import HTTPException
@app.errorhandler(Exception)
def handle_exception(e):
    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e
    # handling non-HTTP exceptions only
    return render_template("Not_Found.html", e=e), 500



# endpoint for Contact page
@app.route('/Contact', methods=["GET","POST"])
def get_contact():
    form = ContactForm()
    if request.method == 'POST':
        name =  request.form["name"]
        email = request.form["email"]
        subject = request.form["subject"]
        message = request.form["message"]
        res = pd.DataFrame({'name':name, 'email':email, 'subject':subject ,'message':message}, index=[0])
        res.to_csv('./contactusMessage.csv')
        return render_template('contact.html', form=form)
    else:
        return render_template('contact.html', form=form)


# endpoint for search page 
@app.route('/search')
def Search1():
    return render_template('Search.html')



# Initialize variables
data = []
labels = []
values = []
variance = []
avg = []
median =[]
mean= []
harmonic_mean = []
low = []
high = []
group = []
pvar = []
dev = []
pst_dev= []

# Create endpoint for "search projects" page (chart1)
@app.route('/search_projects', methods=['GET', 'POST'])
def search1():
    cur = mysql.connection.cursor()
    if request.method == "POST":
        table_projects = request.form['table_projects']
        cur.execute("SELECT Name, Amount, Date from table_projects WHERE Name LIKE %s OR Amount LIKE %s OR Date LIKE %s",
                       (table_projects, table_projects, table_projects))

        mysql.connection.commit()
        global data, labels, values, avg, variance, median, mean, harmonic_mean, low, high, group, pvar, dev, pst_dev

        data = cur.fetchall()
        # print(data)
        data2 = list(data)
        # Τα διαιρώ σε labels και values για να τα χρησιμοποιήσω στα γραφήματα
        labels = [row[2] for row in data2]
        values = [row[1] for row in data2]


        sum_values = sum(values)
        print(sum_values)
        len_values = len(values)
        print(len_values)
        avg = sum_values/len_values
        print(avg)

        mean = st.mean(values)
        print(avg)

        median = st.median(values)
        print(median)

        harmonic_mean = st.harmonic_mean(values)
        print(harmonic_mean)

        low = st.median_low(values)
        print(low)

        high = st.median_high(values)
        print(high)


        group = st.median_grouped(values, interval=1)
        print("group is:", group)

        pvar = st.pvariance(values)
        print(pvar)

        pst_dev = st.pstdev(values)
        print(pst_dev)


        # if len(data) == 0 and table_projects == 'all':
        #     # cur.execute("SELECT Name, Amount, Date from table_projects")
        #     # mysql.connection.commit()
        #     # data = cur.fetchall()
        #     return render_template('chart1.html', data=data)


    return render_template("search_projects.html",data=data, labels=labels, values=values, avg=avg, median=median, 
    harmonic_mean=harmonic_mean, low=low, high=high, group=group, pvar=pvar, pst_dev=pst_dev)



data2 = []
labels2 = []
values2 = []
# endpoint for "search countries" page (chart2)
@app.route('/search_countries', methods=['GET', 'POST'])
def search2():
    cur = mysql.connection.cursor()
    if request.method == "POST":
        table_countries = request.form['table_countries']

        cur.execute("SELECT Country, Name, Sector from table_countries WHERE Country LIKE %s OR Name LIKE %s OR Sector LIKE %s", (table_countries, table_countries, table_countries))
        mysql.connection.commit()
        global data2
        global labels2
        global values2
        data2 = cur.fetchall()
        # Τα data είναι αυτά που εμφανίζονται μετά απο μια αναζήτηση ενός χρήστη
        data3 = list(data2)
        labels2 = [row[0] for row in data3]
        values2 = [row[1] for row in data3]
        # if len(data2) == 0 and table_countries == 'all':
        #     cursor.execute("SELECT Country, Name, Sector from table_countries")
        #     conn.commit()
        #     data2 = cursor.fetchall()
        # return render_template('chart2.html', data=data2)

    return render_template('search_countries.html',data=data2, labels=labels2, values=values2)


# endpoint for "all projects" page 
@app.route('/All_Projects', methods=['GET', 'POST'])
def all_projects():
    cur = mysql.connection.cursor()

    cur.execute("SELECT Name, Amount, Date from table_projects")
    mysql.connection.commit()
    data = cur.fetchall()
    return render_template("All_Projects.html",data=data, labels=labels, values=values)


# endpoint for "all countries" page 
@app.route('/All_Countries', methods=['GET', 'POST'])
def all_countries():
    cur = mysql.connection.cursor()

    cur.execute("SELECT Country, Name, Sector from table_countries")
    mysql.connection.commit()
    data = cur.fetchall()
    return render_template("All_countries.html",data=data, labels=labels, values=values)



@app.route('/More')
def More():

    return render_template("More.html")


@app.route('/Min_projects', methods=['GET', 'POST'])
def min_projects():

    cur = mysql.connection.cursor()

    cur.execute("SELECT Country from table_countries GROUP BY Country HAVING (COUNT(name) = 1)")

    mysql.connection.commit()
    data_min = cur.fetchall()
    return render_template("Min_projects.html", data = data_min)

@app.route('/Max_projects', methods=['GET', 'POST'])
def max_projects():

    cur = mysql.connection.cursor()

    cur.execute("SELECT Country from table_countries GROUP BY Country HAVING (COUNT(name) > 1000)")

    mysql.connection.commit()
    data_max = cur.fetchall()
    return render_template("Max_projects.html", data = data_max)


@app.route('/Max_money', methods=['GET', 'POST'])
def max_money():

    cur = mysql.connection.cursor()

    cur.execute("SELECT table_countries.Name from table_projects inner join table_countries on table_projects.name = table_countries.name where Amount > 1000000000")

    mysql.connection.commit()
    data_max = cur.fetchall()
    return render_template("Max_money.html", data = data_max)


@app.route('/Min_money', methods=['GET', 'POST'])
def min_money():

    cur = mysql.connection.cursor()

    cur.execute("SELECT table_countries.Name from table_projects inner join table_countries on table_projects.name = table_countries.name where Amount < 1000")
    mysql.connection.commit()
    data_max = cur.fetchall()
    return render_template("Min_money.html", data = data_max)

# Run my app
if __name__ == '__main__':
    # app.debug = True
    app.run()




    # cur.execute("SELECT Country from table_countries GROUP BY Country HAVING (COUNT(name) = 1)")

    # cur.execute("SELECT Country from table_countries GROUP BY Country HAVING (COUNT(name) > 1000)")

    # cur.execute("SELECT table_countries.Name from table_projects inner join table_countries on table_projects.name = table_countries.name where Amount > 1000000000")

    # cur.execute("SELECT table_countries.Name from table_projects inner join table_countries on table_projects.name = table_countries.name where Amount < 1000")


# -- Select countries with min frequency of projects 
# select country from table_countries
# GROUP BY country
# having (COUNT(name) = 1);



# -- Select countries with max frequency of projects
# select country from table_countries
# GROUP BY country
# having (COUNT(name) > 1000);


# -- Select projects with > 1 billion 
# select table_countries.Name from table_projects 
# inner join table_countries on table_projects.name = table_countries.name
# where Amount > 1000000000;


# -- Select projects with < 10.000 
# select table_countries.Name from table_projects 
# inner join table_countries on table_projects.name = table_countries.name
# where Amount < 10000;





