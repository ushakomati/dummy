from flask import Flask, render_template, request, url_for, redirect, send_from_directory, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin, login_user, LoginManager, login_required, current_user, logout_user
import os
import pymssql
import pandas as pd
import time
import re
import hashlib

UPLOAD_FOLDER = '/home/azureuser/CC_FinalProject/static/files/'
#UPLOAD_FOLDER = '/Users/anuragmaturu/Downloads/FinalProject/static/files/'
# ALLOWED_EXTENSIONS = {'txt','csv'}

#template_folder = '/Users/anuragmaturu/Downloads/FinalProject/templates/'
template_folder = '/home/azureuser/CC_FinalProject/templates/'

app = Flask(__name__)

server = 'cc-group7finalproject-2022.database.windows.net'
database = 'Kroger_Dataset'
username = 'group7fp'
password = 'MNpw3000*'

app.config['SECRET_KEY'] = 'secret-key-goes-here'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

db = SQLAlchemy(app)

login_manager = LoginManager()
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


##CREATE TABLE
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(1000), unique=True)
    username = db.Column(db.String(1000), unique=True)
    password = db.Column(db.String(1000))
    files_uploaded = db.Column(db.String(255))


with app.app_context():
    db.create_all()


@app.route('/')
def home():
    return render_template("index.html")


@app.route('/register', methods=["GET", "POST"])
def register():
    if request.method == "POST":
        new_user = User(
            email=request.form.get('email'),
            username=request.form.get('username'),
            password=request.form.get('password'),
            files_uploaded='No'
        )

        db.session.add(new_user)
        db.session.commit()

        username_byuser = request.form.get('username')
        password_byuser = request.form.get('password')
        email_byuser = request.form.get('email')

        userdetails_df = pd.DataFrame([[email_byuser, username_byuser, password_byuser, 'No']])

        userdetails_df.columns = ['email', 'username', 'password', 'has_uploaded']

        conn = pymssql.connect(server=server, user=username, password=password, database=database)

        cursor = conn.cursor()

        query = "INSERT INTO app_user VALUES (%s, %s, %s, %s)"

        sql_data = tuple(map(tuple, userdetails_df.values))

        cursor.executemany(query, sql_data)
        conn.commit()
        cursor.close()
        conn.close()

        # Log in and authenticate user after adding details to database.
        login_user(new_user)

        return redirect(url_for('choices'))

    return render_template("register.html")


@app.route('/choices', methods=["GET", "POST"])
@login_required
def choices():
    if request.method == "POST":
        pass
    return render_template("choices.html")

    # username = current_user.username

    # response_cont = current_user.files_uploaded

    # print(username,response_cont)

    # return render_template("choices.html", message=response_cont)


@app.route('/datapullhousenum', methods=["GET", "POST"])
@login_required
def datapullhousenum():
    if request.method == "POST":
        pass
    return render_template("datapull_housenum.html")


@app.route('/demographicfactors', methods=["GET", "POST"])
@login_required
def demographicfactors():
    if request.method == "POST":
        pass
    return render_template("demographicfactors.html")


@app.route('/uploadfiles', methods=["GET", "POST"])
@login_required
def uploadfiles():
    if request.method == "POST":
        user = User.query.filter_by(username=current_user.username).first()

        user_hash = hashlib.md5(current_user.username.encode()).hexdigest()

        file = request.files['households']
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))

        file = request.files['products']
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))

        file = request.files['transactions']
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))

        user.files_uploaded = 'Yes'
        db.session.commit()

        # output_filename = UPLOAD_FOLDER + current_user.filename
        # output_filename_mod = output_filename.replace("_in_", "_out_")
        # fp = open(output_filename_mod, 'x')
        # fp.close()

        conn = pymssql.connect(server=server, user=username, password=password, database=database)

        cursor = conn.cursor()

        cursor.execute("UPDATE [app_user] set [files_uploaded] = 'yes' WHERE [username]=%s", current_user.username)

        conn.commit()
        cursor.close()
        conn.close()

        login_user(user)
        return redirect(url_for('datapullhousenumrecent'))

    return render_template("uploadfiles.html")


@app.route('/datapullhousenumrecent', methods=["GET", "POST"])
@login_required
def datapullhousenumrecent():
    if request.method == "POST":
        housenumber = request.form.get('dropdown')

        username = current_user.username

        user_hash = hashlib.md5(username.encode()).hexdigest()

        files_uploaded = []

        for files in os.listdir(UPLOAD_FOLDER):
            if user_hash in files:
                files_uploaded.append(files)

        firstdataframe = pd.read_csv(UPLOAD_FOLDER + files_uploaded[0])
        seconddataframe = pd.read_csv(UPLOAD_FOLDER + files_uploaded[1])
        thirddataframe = pd.read_csv(UPLOAD_FOLDER + files_uploaded[2])

        firstdataframe.columns = [x.strip().lower() for x in firstdataframe.columns]
        seconddataframe.columns = [x.strip().lower() for x in seconddataframe.columns]
        thirddataframe.columns = [x.strip().lower() for x in thirddataframe.columns]

        if 'basket_num' in [x.lower() for x in thirddataframe.columns]:
            transactions_df = thirddataframe.copy()
        elif 'basket_num' in [x.lower() for x in seconddataframe.columns]:
            transactions_df = seconddataframe.copy()
        else:
            transactions_df = firstdataframe.copy()

        if 'children' in [x.lower() for x in thirddataframe.columns]:
            households_df = thirddataframe.copy()
        elif 'children' in [x.lower() for x in seconddataframe.columns]:
            households_df = seconddataframe.copy()
        else:
            households_df = firstdataframe.copy()

        if 'department' in [x.lower() for x in thirddataframe.columns]:
            products_df = thirddataframe.copy()
        elif 'department' in [x.lower() for x in seconddataframe.columns]:
            products_df = seconddataframe.copy()
        else:
            products_df = firstdataframe.copy()

        default_housenumber = housenumber

        transactions_df_housenumber = transactions_df.loc[
            transactions_df['hshd_num'] == int(default_housenumber)].copy()
        households_df_housenumber = households_df.loc[households_df['hshd_num'] == int(default_housenumber)].copy()

        for i in households_df_housenumber.columns:
            households_df_housenumber[i].fillna('null', inplace=True)

        households_df_obj = households_df_housenumber.select_dtypes(['object'])

        households_df_housenumber[households_df_obj.columns] = households_df_obj.apply(lambda x: x.str.strip())

        for i in transactions_df_housenumber.columns:
            transactions_df_housenumber[i].fillna('null', inplace=True)

        transactions_df_obj = transactions_df_housenumber.select_dtypes(['object'])

        transactions_df_housenumber[transactions_df_obj.columns] = transactions_df_obj.apply(lambda x: x.str.strip())

        for i in products_df.columns:
            products_df[i].fillna('null', inplace=True)

        products_df_obj = products_df.select_dtypes(['object'])

        products_df[products_df_obj.columns] = products_df_obj.apply(lambda x: x.str.strip())

        households_df_housenumber.rename(columns={"hshd_num": "hshd_num_h"}, inplace=True)

        products_df.rename(columns={"product_num": "product_num_p"}, inplace=True)

        intermediate_result = pd.merge(transactions_df_housenumber, households_df_housenumber, left_on='hshd_num',
                                       right_on='hshd_num_h', how='inner')

        result = pd.merge(intermediate_result, products_df, left_on='product_num', right_on='product_num_p',
                          how='inner')

        result_housenumber = result[
            ['hshd_num', 'basket_num', 'purchase_', 'product_num', 'department', 'commodity', 'spend', 'units',
             'store_r', 'week_num', 'year', 'l', 'age_range', 'marital', 'income_range', 'homeowner',
             'hshd_composition', 'hh_size', 'children']]

        result_housenumber.columns = ['Hshd_num', 'Basket_num', 'Date', 'Product_num', 'Department', 'Commodity',
                                      'Spend', 'Units', 'Store_region', 'Week_num', 'Year', 'Loyalty_flag', 'Age_range',
                                      'Marital_status', 'Income_range', 'Homeowner', 'Hshd_composition', 'Hsh_size',
                                      'Children']

        result_housenumber = result_housenumber.sort_values(by=['Basket_num', 'Date', 'Product_num', 'Department', 'Commodity', 'Spend'], ignore_index = True)

        # render dataframe as html
        files_uploaded_html = result_housenumber.to_html()

        html_files_uploaded = []

        for files in os.listdir(template_folder):
            if 'datapullhousenumrecent' in files:
                html_files_uploaded.append(files)

        html_int = []

        if len(html_files_uploaded) == 0:
            new_file_name = 'datapullhousenumrecent.html'
        else:
            for file in html_files_uploaded:
                if re.sub("\.html", "", re.sub("datapullhousenumrecent", "", file)) == '':
                    html_int.append(0)
                else:
                    html_int.append(int(re.sub("\.html", "", re.sub("datapullhousenumrecent", "", file))))
            new_file_name = 'datapullhousenumrecent' + str(max(html_int) + 1) + '.html'

        # write html to file
        text_file = open(template_folder + new_file_name, "w")
        text_file.write('{% extends "base.html" %}\n')
        text_file.write('{% block content %}\n')
        text_file.write('<body>\n')
        text_file.write('<a href="{{ url_for(' + "'choices'" + ') }}" class="btn btn-primary btn-large">Click here to get back to choice selection</a>\n')
        text_file.write("<h3 align=" + '"left"' + ">Here's the list of Household's. Select any one:</h3>\n")
        text_file.write('<form action="{{ url_for(' + "'datapullhousenumrecent'" + ') }}" method="post">\n')
        text_file.write('<select name = "dropdown">\n')

        for index in sorted(list(households_df["hshd_num"].unique())):
            if index != int(default_housenumber):
                text_file.write('<option value = "' + str(index) + '">' + str(index) + '</option>\n')
            else:
                text_file.write('<option value = "' + str(index) + '" selected>' + str(index) + '</option>\n')
        text_file.write('</select>\n')
        text_file.write('<button type="submit" class="btn btn-primary btn-large">Submit</button>\n')
        text_file.write('</form>\n')
        text_file.write('<br>\n')
        text_file.write('<br>\n')
        text_file.write('<br>\n')
        text_file.write(files_uploaded_html)
        text_file.write('</body>')
        text_file.write('{% endblock %}\n')
        text_file.close()

        time.sleep(2)

        return render_template(new_file_name)

    username = current_user.username

    user_hash = hashlib.md5(username.encode()).hexdigest()

    files_uploaded = []

    for files in os.listdir(UPLOAD_FOLDER):
        if user_hash in files:
            files_uploaded.append(files)

    firstdataframe = pd.read_csv(UPLOAD_FOLDER + files_uploaded[0])
    seconddataframe = pd.read_csv(UPLOAD_FOLDER + files_uploaded[1])
    thirddataframe = pd.read_csv(UPLOAD_FOLDER + files_uploaded[2])

    firstdataframe.columns = [x.strip().lower() for x in firstdataframe.columns]
    seconddataframe.columns = [x.strip().lower() for x in seconddataframe.columns]
    thirddataframe.columns = [x.strip().lower() for x in thirddataframe.columns]

    if 'basket_num' in [x.lower() for x in thirddataframe.columns]:
        transactions_df = thirddataframe.copy()
    elif 'basket_num' in [x.lower() for x in seconddataframe.columns]:
        transactions_df = seconddataframe.copy()
    else:
        transactions_df = firstdataframe.copy()

    if 'children' in [x.lower() for x in thirddataframe.columns]:
        households_df = thirddataframe.copy()
    elif 'children' in [x.lower() for x in seconddataframe.columns]:
        households_df = seconddataframe.copy()
    else:
        households_df = firstdataframe.copy()

    if 'department' in [x.lower() for x in thirddataframe.columns]:
        products_df = thirddataframe.copy()
    elif 'department' in [x.lower() for x in seconddataframe.columns]:
        products_df = seconddataframe.copy()
    else:
        products_df = firstdataframe.copy()

    default_housenumber = int(households_df["hshd_num"].iloc[0:1, ].values)

    transactions_df_housenumber = transactions_df.loc[transactions_df['hshd_num'] == default_housenumber].copy()
    households_df_housenumber = households_df.loc[households_df['hshd_num'] == default_housenumber].copy()

    for i in households_df_housenumber.columns:
        households_df_housenumber[i].fillna('null', inplace=True)

    households_df_obj = households_df_housenumber.select_dtypes(['object'])

    households_df_housenumber[households_df_obj.columns] = households_df_obj.apply(lambda x: x.str.strip())

    for i in transactions_df_housenumber.columns:
        transactions_df_housenumber[i].fillna('null', inplace=True)

    transactions_df_obj = transactions_df_housenumber.select_dtypes(['object'])

    transactions_df_housenumber[transactions_df_obj.columns] = transactions_df_obj.apply(lambda x: x.str.strip())

    for i in products_df.columns:
        products_df[i].fillna('null', inplace=True)

    products_df_obj = products_df.select_dtypes(['object'])

    products_df[products_df_obj.columns] = products_df_obj.apply(lambda x: x.str.strip())

    households_df_housenumber.rename(columns={"hshd_num": "hshd_num_h"}, inplace=True)

    products_df.rename(columns={"product_num": "product_num_p"}, inplace=True)

    intermediate_result = pd.merge(transactions_df_housenumber, households_df_housenumber, left_on='hshd_num',
                                   right_on='hshd_num_h', how='inner')

    result = pd.merge(intermediate_result, products_df, left_on='product_num', right_on='product_num_p', how='inner')

    result_housenumber = result[
        ['hshd_num', 'basket_num', 'purchase_', 'product_num', 'department', 'commodity', 'spend', 'units',
         'store_r', 'week_num', 'year', 'l', 'age_range', 'marital', 'income_range', 'homeowner',
         'hshd_composition', 'hh_size', 'children']]

    result_housenumber.columns = ['Hshd_num', 'Basket_num', 'Date', 'Product_num', 'Department', 'Commodity',
                                  'Spend', 'Units', 'Store_region', 'Week_num', 'Year', 'Loyalty_flag', 'Age_range',
                                  'Marital_status', 'Income_range', 'Homeowner', 'Hshd_composition', 'Hsh_size',
                                  'Children']

    result_housenumber = result_housenumber.sort_values(
        by=['Basket_num', 'Date', 'Product_num', 'Department', 'Commodity', 'Spend'], ignore_index=True)

    # render dataframe as html
    files_uploaded_html = result_housenumber.to_html()

    html_files_uploaded = []

    for files in os.listdir(template_folder):
        if 'datapullhousenumrecent' in files:
            html_files_uploaded.append(files)

    html_int = []

    if len(html_files_uploaded) == 0:
        new_file_name = 'datapullhousenumrecent.html'
    else:
        for file in html_files_uploaded:
            if re.sub("\.html", "", re.sub("datapullhousenumrecent", "", file)) == '':
                html_int.append(0)
            else:
                html_int.append(int(re.sub("\.html", "", re.sub("datapullhousenumrecent", "", file))))
        new_file_name = 'datapullhousenumrecent' + str(max(html_int) + 1) + '.html'

    # write html to file
    text_file = open(template_folder + new_file_name, "w")
    text_file.write('{% extends "base.html" %}\n')
    text_file.write('{% block content %}\n')
    text_file.write('<body>\n')
    text_file.write('<a href="{{ url_for('+"'choices'"+') }}" class="btn btn-primary btn-large">Click here to get back to choice selection</a>\n')
    text_file.write("<h3 align=" + '"left"' + ">Here's the list of Household's. Select any one:</h3>\n")
    text_file.write('<form action="{{ url_for(' + "'datapullhousenumrecent'" + ') }}" method="post">\n')
    text_file.write('<select name = "dropdown">\n')
    for index in sorted(list(households_df["hshd_num"].unique())):
        if index != default_housenumber:
            text_file.write('<option value = "' + str(index) + '">' + str(index) + '</option>\n')
        else:
            text_file.write('<option value = "' + str(index) + '" selected>' + str(index) + '</option>\n')
    text_file.write('</select>\n')
    text_file.write('<button type="submit" class="btn btn-primary btn-large">Submit</button>\n')
    text_file.write('</form>\n')
    text_file.write('<br>\n')
    text_file.write('<br>\n')
    text_file.write('<br>\n')
    text_file.write(files_uploaded_html)
    text_file.write('</body>')
    text_file.write('{% endblock %}\n')
    text_file.close()

    return render_template(new_file_name)


@app.route('/login', methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get('username')
        password = request.form.get('password')

        # Find user by email entered.
        user = User.query.filter_by(username=username).first()

        if user:
            # Check stored password hash against entered password hashed.
            if user.password == password:
                login_user(user)
                return redirect(url_for('choices'))
        else:
            return render_template("login.html")

    return render_template("login.html")


@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('home'))


if __name__ == "__main__":
    # app.run(host='0.0.0.0',port=8080)
    # app.run(debug = True)
    app.run(host='0.0.0.0', port=8080)
