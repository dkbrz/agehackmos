import sqlite3
import pandas as pd
from flask import Flask, render_template, request, g
from queries import EXISTING_QUERY, CAT_INFO, GROUP_INFO, HISTORY, QUESTIONNAIRE_GROUPS, QUESTIONNAIRE_CAT

app = Flask(__name__)
DATABASE = 'app.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# db = SQLAlchemy(app)

def get_db():
	db = getattr(g, '_database', None)
	if db is None:
		db = g._database = sqlite3.connect(DATABASE)
		db.row_factory = sqlite3.Row
	return db


@app.teardown_appcontext
def close_connection(exception):
	db = getattr(g, '_database', None)
	if db is not None:
		db.close()


def query_db(query, args=(), one=False):	
	cur = get_db().execute(query, args)
	rv = cur.fetchall()
	cur.close()
	return (rv[0] if rv else None) if one else rv


def data_to_page(draft):
	data = {'mind': [], 'body': [], 'soul': []}
	for name, cat in [('Для ума', 'mind'), ('Для тела', 'body'), ('Для души', 'soul')]:
		data[cat] = [
			{
				'id': draft.loc[i]['group_id'],
				'l1': draft.loc[i]['l1'],
				'l2': draft.loc[i]['l2'],
				'l3': draft.loc[i]['l3'],
				'a': draft.loc[i]['address'],
				'o': draft.loc[i]['is_online'],
				't': [
					draft.loc[i]['monday'] if draft.loc[i]['monday'] > 0 else '',
					draft.loc[i]['tuesday'] if draft.loc[i]['tuesday'] > 0 else '',
					draft.loc[i]['wednesday'] if draft.loc[i]['wednesday'] > 0 else '',
					draft.loc[i]['thursday'] if draft.loc[i]['thursday'] > 0 else '',
					draft.loc[i]['friday'] if draft.loc[i]['friday'] > 0 else '',
					draft.loc[i]['saturday'] if draft.loc[i]['saturday'] > 0 else '',
					draft.loc[i]['sunday'] if draft.loc[i]['sunday'] > 0 else ''
				],
				'i': draft.loc[i]['num'],
				'n': draft.loc[i]['same_post']
			}
			for i in draft[draft['category'] == name].head(10).index
		]
	return data


@app.route('/')
def index():
	return render_template("index.html")


@app.route('/existing')
def existing():
	if request.values:
		user_id = request.values.get("user_id", int)
	
	user = query_db("SELECT age_group, is_woman FROM users WHERE user_id = ?", args=(user_id,), one=True)
	result = query_db(
		EXISTING_QUERY, 
		args=(user_id, user_id, user_id, user_id, user_id, user_id, user_id)
	)
	
	groups = pd.DataFrame(result, columns=[
		'group_id', 'is_online', 'category_id', 'same_district', 'same_zone',
		'same_post', 'n_neighbors', 'total_rank', 'offline_rank', 'online_rank',
		'ends_soon'
	])
	groups = groups.drop_duplicates(subset=['category_id'], keep='first')
	groups['num'] = range(1, groups.shape[0] + 1)
	
	cat_info = pd.DataFrame(query_db(CAT_INFO), columns=['category_id', 'l1', 'l2', 'l3', 'category'])
	
	group_info = pd.DataFrame(
		query_db(GROUP_INFO), 
		columns=['group_id', 'is_online', 'address', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
	).drop_duplicates(subset='group_id', keep='first')
	
	groups = groups.merge(cat_info).merge(group_info)
	data = data_to_page(groups)
	
	history = pd.DataFrame(query_db(HISTORY, args=(user_id,)), columns=['l1', 'l2', 'l3', 'start', 'finish', 'n'])
	
	return render_template("results.html", user=user, data=data, history=history.to_dict('records'))

@app.route('/new')
def new():
	if request.values:
		postalcode = request.values.get("postalcode", int)
		age_group = max(min((2023 - int(request.values.get("birthyear", int))) // 10 * 10, 90), 50)
		gender = 1 if request.values.get("gender") == 'f' else 0
		ok_cats = {i[0] for i in request.values.items() if i[1] == '1'}
		online = 1 if request.values.get("online") == '1' else 0
		offline = 1 if request.values.get("offline") == '1' else 0
		near = 1 if request.values.get("near") == '1' else 0
	
	result = query_db(
		QUESTIONNAIRE_GROUPS, 
		args=(postalcode, age_group, gender)
	)

	groups = pd.DataFrame(result, columns=[
		'group_id', 'is_online', 'category_id', 'same_district', 'same_zone',
		'same_post', 'n_neighbors', 'total_rank'
	])

	if online == 1:
		groups = groups[groups['is_online'] == 1]
	if offline == 1:
		groups = groups[groups['is_online'] == 0]
	if near == 1:
		groups = groups[groups['same_zone'] == 1]

	groups = groups.drop_duplicates(subset=['category_id'], keep='first')
	groups['num'] = range(1, groups.shape[0] + 1)

	group_info = pd.DataFrame(
		query_db(GROUP_INFO), 
		columns=['group_id', 'is_online', 'address', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
	).drop_duplicates(subset='group_id', keep='first')
	cat_info = pd.DataFrame(query_db(CAT_INFO), columns=['category_id', 'l1', 'l2', 'l3', 'category'])
	
	groups = groups.merge(cat_info).merge(group_info)

	cats = pd.DataFrame(query_db(QUESTIONNAIRE_CAT), columns=['category_id', 'feature'])
	if len(ok_cats) > 0:
		cats = cats[cats['feature'].isin(ok_cats)]
		groups = groups[groups['category_id'].isin(cats['category_id'])]

	data = data_to_page(groups)
	return render_template("results.html", data=data, user=(age_group, gender))

if __name__ == '__main__':
	app.run(debug=True, port=5001)