import flask
import pathlib, tempfile, sys
from flask_mysqldb import MySQLdb
from upco_tools import upco_ale, upco_timecode, upco_diva
from ..db import db
from ..resources import dailies as db_dailies


# TAPEWORM Rest API - Diva Extension
# ========================================================================
# Currently v1

diva = flask.Blueprint("diva", __name__, url_prefix="/diva")

@diva.route("/v1/shows/")
def list_shows_rest():
	
	cur = db.connection.cursor()
	
	# Generic show lookup
	cur.execute("""
		SELECT
			bin_to_uuid(shows.guid_show) as guid_show,
			shows.title,
			diva.category as category,
			diva.media_group as media_group,
			diva.src_dest as src_dest,
			diva.tv_path as tv_path
		FROM
			dailies_shows shows
		INNER JOIN
			diva_config diva on diva.guid_show = shows.guid_show
		ORDER BY
			title
	""")
	results = cur.fetchall()

	return flask.jsonify(results)

@diva.route("/v1/shows/<string:guid_show>/shots/")
def list_shots_rest(guid_show):

	cur = db.connection.cursor()

	cur.execute("""
		SELECT * FROM view_divainfo WHERE guid_show = %s
	""", (guid_show,))

	results = cur.fetchall()

	return flask.jsonify(results)