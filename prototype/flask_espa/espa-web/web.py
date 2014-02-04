#!/usr/bin/env python

from flask import Flask, render_template
from flask import abort, redirect, url_for
from flask import request, make_response
from flask import session, escape
import logic

app = Flask(__name__)

#############################################
#Login, Logout, Session Management
#############################################


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if logic.authenticate(request.form['username'], request.form['password']):
            session['username'] = request.form['username']
            if 'next_page' in request.args:
                return redirect(url_for(request.args['next_page']))
            else:
                return redirect(url_for('index'))
        else:
            return render_template('login.html',
                                   msg="Invalid username & password combination. \
                                        Please try again.")
    else:
        return render_template('login.html')


@app.route('/logout')
def logout():
    session.pop('username', None)
    return render_template('loggedout.html')


def username():
    return session.get('username')


def authorize(session,role):
    if session.has_key('username'):
        return logic.authorize(session['username'], role)
    else:
        return False
                  

#############################################
# Error pages
#############################################

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(e):
    return render_template('500.html'), 500


#############################################
# Admin pages
#############################################


@app.route('/admin')
def admin():
    if not authorize(session, 'admin'):
        return redirect(url_for('login'))
    return "Hit /admin"

@app.route('/admin/orders/')
@app.route('/admin/orders/<username>')
@app.route('/admin/orders/<username>/<order_number>')
def admin_orders(username=None, order_number=None):
    if not is_authorized(session, 'admin'):
        return redirect(url_for('login'))
    
    return "Hit /admin/orders"


@app.route('/admin/scenes/<scene_name>')
def admin_scenes(scene_name=None):
    if not is_authorized(session, 'admin'):
        return redirect(url_for('login'))
    return "Hit /admin/scenes"


#############################################
#Index, Landing and Welcome Pages
#############################################


@app.route('/')
def index():
    username = session.get('username')
    return render_template('index.html', username=username)


@app.route('/order/new')
def new_order():
    pass

@app.route('/order/status/<email>/<order_number>')
def order_status(email, order_number=None):
    pass

@app.route('/docs')
@app.route('/docs/<doc_name>')
def docs(doc_name=None):
    return render_template("docs.html")

@app.route('/api')
def apps():
    return render_template("api.html")


'''
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
'''

############################################
# Script execution
############################################


if __name__ == "__main__":
    app.secret_key = 'abc123'
    app.debug = True
    app.run()
                        
