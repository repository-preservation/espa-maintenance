#!/usr/bin/env python
from flask import Flask, render_template
from flask import abort, redirect, url_for
from flask import request, make_response
from flask import session, escape
import security
app = Flask(__name__)


if __name__ == "__main__":
    app.secret_key = 'abc123'
    app.debug = True
    app.run() 
