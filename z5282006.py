import pandas as pd
from flask import Flask, send_file
from flask import request
from flask_restx import Resource, Api
from flask_restx import fields
from flask_restx import reqparse
import sqlite3
import requests
from datetime import datetime
import re
import matplotlib.pyplot as plt

app = Flask(__name__)
api = Api(app,
          default="TV shows",
          title="TV show Dataset",
          description="Assignment2 - Data Service for TV Shows"
          )

HOST_NAME = '127.0.0.1'
PORT = '5000'


def read_db():
    try:
        conn = sqlite3.connect('z5282006.db')
        return pd.read_sql('select * from z5282006', conn)
    except:
        return None


def write_db(df):
    conn = sqlite3.connect('z5282006.db')
    df.to_sql('z5282006', con=conn, if_exists='replace', index=False)


# The following is the schema of tv show
show_model = api.model('tvshow', {
    'tvmaze-id': fields.Integer,
    'id': fields.Integer,
    'last-update': fields.String,
    'name': fields.String,
    'type': fields.String,
    'language': fields.String,
    'genres': fields.String,  # List
    'status': fields.String,
    'runtime': fields.Integer,
    'premiered': fields.String,
    'officialSite': fields.String,
    'schedule': fields.Raw,
    'rating': fields.Raw,
    'weight': fields.Integer,
    'network': fields.Raw,
    'summary': fields.String,
    '_links': fields.Raw
})

# Question1 parser
parser = reqparse.RequestParser()
parser.add_argument('name', required=True)

# Question5 parser
parser5 = reqparse.RequestParser()
parser5.add_argument('order_by', default='+id')
parser5.add_argument('page', default='1')
parser5.add_argument('page_size', default='100')
parser5.add_argument('filter', default='id,name')

# Question6 parser
parser6 = reqparse.RequestParser()
parser6.add_argument('format')
parser6.add_argument('by')


@api.route('/tv-shows/import')
class Q1(Resource):

    @api.response(200, 'Successful')
    @api.doc(description="Import books")
    @api.expect(parser)
    def post(self):
        # get show as JSON string
        args = parser.parse_args()

        # retrieve the query parameters
        name = args.get('name')
        name = name.lower()
        name = re.sub('[^a-z0-9]+', ' ', name)

        # read db
        shows_df = read_db()

        # if the show is in db
        if shows_df is not None and name in shows_df['name'].values:
            show_df = shows_df[shows_df['name'] == name]
            links = {'self': {
                "href": "http://{}:{}/tv-shows/{}".format(HOST_NAME, PORT, int(''.join(show_df['id'].values)))}}
            return {"id": int(''.join(show_df['id'].values)),
                    "last-update": "{}".format(''.join(show_df['updated'].values)),
                    "tvmaze-id": int(''.join(show_df['tvmaze-id'].values)),
                    "_links": links
                    }

        # if the show is not in db, import it
        url = "http://api.tvmaze.com/search/shows?q=" + name
        response = requests.get(url)
        content = response.json()
        shows = pd.DataFrame(content)
        # store the show with highest score
        shows.sort_values(by='score', inplace=True, ascending=False)
        show = shows.iloc[0]
        show_inf = show['show']
        df = pd.DataFrame([show_inf])
        # some preprocess
        df.rename(columns={'id': 'tvmaze-id'}, inplace=True)
        df['name'] = df['name'].str.lower()
        df['updated'] = df['updated'].apply(lambda x: datetime.fromtimestamp(x))

        if name != df['name'].values:
            return {"message": "Have not found this tv show"}

        # a unique integer identifier automatically generated
        if shows_df is None:
            df['id'] = 1
            df = df.applymap(str)
            write_db(df)
        else:
            last_id = shows_df['id'].iloc[-1]
            df['id'] = eval(last_id) + 1
            new_df = shows_df.append(df, ignore_index=True)
            df = df.applymap(str)
            new_df = new_df.applymap(str)
            write_db(new_df)

        # return value: links
        links = {'self': {"href": "http://{}:{}/tv-shows/{}".format(HOST_NAME, PORT, int(''.join(df['id'].values)))}}

        return {"id": int(''.join(df['id'].values)),
                "last-update": "{}".format(''.join(df['updated'].values)),
                "tvmaze-id": int(''.join(df['tvmaze-id'].values)),
                "_links": links
                }, 201


@api.route('/tv-shows/<int:id>')
@api.param('id', 'The TV show identifier')
class Q234(Resource):
    @api.response(404, 'TV show is not found')
    @api.response(200, 'Successful')
    @api.doc(description="Get a TV show by its id")
    def get(self, id):
        # read from db
        showslist = read_db()
        id = str(id)

        # show not in db
        if id not in showslist['id'].values:
            api.abort(404, "TV show {} doesn't exist".format(id))

        df = showslist[showslist['id'] == id]

        # return values
        ret = {'tvmaze-id': int(''.join(df['tvmaze-id'].values)), 'id': int(''.join(df['id'].values)),
               'last-update': "{}".format(''.join(df['updated'].values)),
               'name': "{}".format(''.join(df['name'].values)), 'type': "{}".format(''.join(df['type'].values)),
               'language': "{}".format(''.join(df['language'].values)), 'genres': eval(''.join(df['genres'])),
               'status': "{}".format(''.join(df['status'].values)), 'runtime': int(''.join(df['runtime'].values)),
               'premiered': "{}".format(''.join(df['premiered'].values)),
               'officialSite': "{}".format(''.join(df['officialSite'].values)),
               'schedule': eval(''.join(df['schedule'].values)), 'rating': eval(''.join(df['rating'].values)),
               'weight': int(''.join(df['weight'].values)), 'network': eval(''.join(df['network'].values)),
               'summary': "{}".format(''.join(df['summary'].values)), '_links': {"self": {
                "href": "http://{}:{}/tv-shows/{}".format(HOST_NAME, PORT, int(''.join(df['id'].values)))
            },
                "next": {
                    "href": "http://{}:{}/tv-shows/{}".format(HOST_NAME, PORT, int(''.join(df['id'].values)) + 1)
                }}}
        if int(''.join(df['id'].values)) > 1:
            ret['_links']['previous'] = {
                "href": "http://{}:{}/tv-shows/{}".format(HOST_NAME, PORT, int(''.join(df['id'].values)) - 1)}

        return ret, 200

    @api.response(404, 'TV show was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Delete a TV show by its ID")
    def delete(self, id):
        # read from db
        showslist = read_db()
        id = str(id)

        # show not in db
        if id not in showslist['id'].values:
            api.abort(404, "TV show {} doesn't exist".format(id))

        # delete
        showslist.drop(showslist[showslist.id == id].index, inplace=True)
        write_db(showslist)
        return {"message": "The tv show with id {} was removed from the database!".format(id),
                "id": int(id)
                }, 200

    @api.response(404, 'tv show was not found')
    @api.response(400, 'Validation Error')
    @api.response(200, 'Successful')
    @api.expect(show_model, validate=True)
    @api.doc(description="Update a tv show by its ID")
    def patch(self, id):
        # read fro db
        showslist = read_db()
        id = str(id)

        # tv show not in db
        if id not in showslist['id'].values:
            api.abort(404, "tv show {} doesn't exist".format(id))

        # get the payload and convert it to a JSON
        tvshow = request.json

        # show ID cannot be changed
        if 'id' in tvshow and id != tvshow['id']:
            return {"message": "Id of the tv show cannot be changed".format(id)}, 400

        # Update the values
        showslist.set_index('id', inplace=True)
        for key in tvshow:
            if key not in show_model.keys():
                # unexpected column
                return {"message": "Property {} is invalid".format(key)}, 400
            showslist.loc[id, key] = tvshow[key]
        showslist.loc[id, 'updated'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        showslist.reset_index(inplace=True)

        # write to db
        write_db(showslist)

        # return values
        df = showslist[showslist['id'] == id]
        links = {'self': {
            "href": "http://{}:{}/tv-shows/{}".format(HOST_NAME, PORT, int(''.join(df['id'].values)))}}

        return {"id": int(''.join(df['id'].values)),
                "last-update": "{}".format(''.join(df['updated'].values)),
                "_links": links
                }, 200


@api.route('/tv-show')
class Q5(Resource):

    @api.response(200, 'Successful')
    @api.doc(description="Retrieve the list of available TV Shows")
    @api.expect(parser5)
    def get(self):
        # read from db
        showslist = read_db()

        # get shows as JSON string
        args = parser5.parse_args()

        # retrieve the query parameters
        order_by = args.get('order_by')
        page = args.get('page')
        page_size = args.get('page_size')
        flt = args.get('filter')

        # sort
        if order_by:
            order_list = []
            order_asc = []
            comms = order_by.split(',')
            for comm in comms:
                symbol = comm[0]
                order_list.append(comm[1:])
                if symbol == '+':
                    order_asc.append('True')
                else:
                    order_asc.append('False')
            showslist.sort_values(by=order_list, inplace=True, ascending=tuple(order_asc))

        # filter
        if flt:
            flt_list = flt.split(',')
            showslist = showslist[flt_list]

        # tv show content
        begin = (int(page) - 1) * int(page_size)
        end = int(page) * int(page_size)
        showslist = showslist.iloc[begin:end]
        tvshows = showslist.to_dict('records')

        # links
        links = {'self': {"href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter"
                                  "={}".format(HOST_NAME, PORT, order_by, page, page_size, flt)},
                 'next': {"href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter"
                                  "={}".format(HOST_NAME, PORT, order_by, int(page) + 1, page_size, flt)}}
        if int(page) > 1:
            links['previous'] = {"href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter"
                                         "={}".format(HOST_NAME, PORT, order_by, int(page) - 1, page_size, flt)}

        return {"page": page,
                "page-size": page_size,
                "tv-shows": tvshows,
                "_links": links}, 200


@api.route('/tv-show/statistics')
class Q6(Resource):
    @api.response(200, 'Successful')
    @api.doc(description="Get the statistics of the existing TV Show")
    @api.expect(parser6)
    def get(self):
        # read from db
        showslist = read_db()

        # get shows as JSON string
        args = parser6.parse_args()

        # retrieve the query parameters
        fmt = args.get('format')
        by = args.get('by')

        # Total Number of TV shows
        total = len(showslist)

        # percentage
        values = showslist[by].value_counts() / total * 100

        # Total Number of TV shows updated in the last 24 hours
        total_updated = 0
        now = datetime.now()
        for t in showslist['updated'].values:
            updated = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
            interval = now - updated
            if interval.days < 1:
                total_updated += 1

        if fmt == 'json':
            return {"total": total,
                    "total-updated": total_updated,
                    "values": values.to_dict()}, 200

        if fmt == 'image':
            plt.cla()
            values.plot.pie(subplots=True)
            plt.savefig('./z5282006.jpg')
            return send_file('z5282006.jpg', mimetype='image/jpeg')


if __name__ == '__main__':
    # run the application
    app.run(debug=True)
