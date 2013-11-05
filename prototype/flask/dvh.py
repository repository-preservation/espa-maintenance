#!/usr/bin/env python

from flask import Flask, render_template
from flask import abort, redirect, url_for
from flask import request, make_response
from flask import session, escape
import security as s
import os, sys

app = Flask(__name__)

#############################################
#Login, Logout, Session Management
#############################################

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if s.authenticate(request.form['username'], request.form['password']):
            session['username'] = request.form['username']
            next_page = request.form['next']
            if not next_page:
                next_page = 'index'
            return redirect(url_for(next_page))
        else:
            return render_template('login.html', msg="That username & password combo doesn't work Bobalooga")
    else:
        return render_template('login.html', next=request.args.get('next'))

@app.route('/logout')
def logout():
    session.pop('username', None)
    return render_template('loggedout.html')

def get_username():
    return session.get('username')

def is_authorized(session,role):
    if not session.has_key('username'):
        return False
    elif s.is_user_in_role(session['username'], role):
        return True
    else:
        return False
    

#############################################
#Index, Landing and Welcome Pages
#############################################


@app.route('/')
def index():
    username = session.get('username')
    return render_template('index.html', username=username)

@app.route('/template')
def template():
    return render_template('dvh_template.html')

@app.route('/admin')
def admin():
    if not is_authorized(session, 'admin'):
        return redirect(url_for('login', next='admin'))
    return "Hit /admin"

@app.route('/admin/orders/')
@app.route('/admin/orders/<username>')
@app.route('/admin/orders/<username>/<order_number>')
def admin_orders(username=None, order_number=None):
    if not is_authorized(session, 'admin'):
        return redirect(url_for('login', next='admin_orders'))
    
    return "Hit /admin/orders"


@app.route('/admin/scenes/<scene_name>')
def admin_scenes(scene_name=None):
    if not is_authorized(session, 'admin'):
        return redirect(url_for('login', next='admin_scenes'))
    return "Hit /admin/scenes"


@app.route('/docs/')
@app.route('/docs/<doc_name>')
def documentation(doc_name=None):
    return "Documentation"

@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    if 'username' in session:
        _name = session['username']
    elif name:
        _name = name
    else:
        _name = None
    return render_template("hello.html", name=_name)



############################################
# Script execution
############################################


if __name__ == "__main__":
    app.config.from_pyfile(os.getenv("DVHCONFIG", "configuration.default"))
    app.run()
                        
