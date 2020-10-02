import flask
import pathlib, tempfile, sys
from flask_mysqldb import MySQLdb
from upco_tools import upco_ale, upco_timecode, upco_diva
from ..db import db
from ..resources import dailies as db_dailies


# TAPEWORM Rest API
# ========================================================================
# Currently v1

dailies = flask.Blueprint("dailies", __name__, url_prefix="/dailies")

@dailies.route("/v1/shows/", methods=["POST","GET"])
def list_shows_rest():
	
	cur = db.connection.cursor()

	print(flask.request.method)
	
	# Generic show lookup
	if flask.request.method == "GET":
		cur.execute("SELECT bin_to_uuid(guid_show) as guid_show, title FROM dailies_shows ORDER BY title")
		results = cur.fetchall()

	# Specific show
	# TODO: Split into searchShows method
	elif flask.request.method == "POST":

		if flask.request.form.get("guid_show") is not None:
			cur.execute("SELECT bin_to_uuid(guid_show) as guid_show, title FROM dailies_shows WHERE guid_show=uuid_to_bin(%s) LIMIT 1", (flask.request.form.get("guid_show"),))
			results = cur.fetchone()

		elif flask.request.form.get("title") is not None:
			cur.execute("SELECT bin_to_uuid(guid_show) as guid_show, title FROM dailies_shows WHERE title=%s LIMIT 1",(flask.request.form.get("title"),))
			results = cur.fetchone()
		
		elif flask.request.form.get("title_search") is not None:
			cur.execute("SELECT bin_to_uuid(guid_show) as guid_show, title FROM dailies_shows WHERE title LIKE %s", (f"%{flask.request.form.get('title_search')}%",))
			results = cur.fetchall()
	
	return flask.jsonify(results)
	
# NOTE: Recent change fetchall()->fetchone()
@dailies.route("/v1/shows/<string:show>")
def list_show_details_rest(show):
	
	cur = db.connection.cursor()
	cur.execute("SELECT bin_to_uuid(guid_show) as guid_show, title FROM dailies_shows WHERE guid_show = uuid_to_bin(%s) LIMIT 1", (show,))
	results = cur.fetchone()
	
	return flask.jsonify(results)

# NOTE: Recent change, may break some things
@dailies.route("/v1/shows/<string:show>/shots/")
def list_shots_by_show(show):

	try:
		results = db_dailies.getShots(guid_show=show)
	except Exception as e:
		print(f"MySQL error in list_shots_by_show: {e}")
		results = []

	return flask.jsonify(results)

@dailies.route("/v1/shows/<string:show>/selects/add", methods=["POST"])
def addSelect(show):

	shot_info = flask.request.json
	
	try:
		results = db_dailies.addSelect(shot_info.get("guid_show"), shot_info.get("shot_name"), shot_info.get("frm_start"), shot_info.get("frm_end"), shot_info.get("selects_reel"))
	except Exception as e:
		print("Error: ",e)
		results = []

	return flask.jsonify(results)


@dailies.route("/v1/shows/<string:show>/selects/search", methods=["POST"])
def searchSelects(show):

	search_info = flask.request.json
	
	try:
		result = db_dailies.searchSelects(search_info)
	except Exception as e:
		print("Error: ",e)

	return flask.jsonify(result)





# NOTE: Recent change, may break some things
@dailies.route("/v1/shots/<string:guid>/")
def get_shot_by_guid(guid):
	print("Usin the new one")
	try:
		results = db_dailies.getShots(guid_shot=guid)
	except Exception as e:
		print(f"MySQL error in list_shots_by_show: {e}")
		results = []
	
	return flask.jsonify(results)


@dailies.route("/v1/shots/", methods=["POST","GET"])
def find_shot():
	
	cur = db.connection.cursor()
	
	# Generic show lookup
	#if flask.request.method == "GET":
	#	cur.execute("SELECT bin_to_uuid(guid_show) as guid_show, title FROM dailies_shows ORDER BY title")

	# Specific show
	# TODO: Split into searchShows method
	if flask.request.method == "POST":

		if flask.request.json.get("guid_shot") is not None:
			cur.execute("""
				SELECT
					bin_to_uuid(s.guid_shot) as guid_shot,
					bin_to_uuid(s.guid_show) as guid_show,
					s.shot as shot,
					s.frm_start as frm_start,
					s.frm_duration as frm_duration,
					s.frm_end as frm_end,
					IFNULL(m.extended_info, JSON_OBJECT()) as metadata
				FROM dailies_shots s
				LEFT JOIN dailies_metadata m ON m.guid_shot = s.guid_shot
				WHERE s.guid_shot = uuid_to_bin(%s)
				LIMIT 1
			""", (flask.request.form.get("guid_shot"),))
			results = cur.fetchone()

		else:
			search_query  = []
			search_params = []
			subclip = bool(flask.request.json.get("subclip"))

			# Check for show
			if flask.request.json.get("guid_show") is not None:
				print("Limiting to show guid", flask.request.json.get("guid_show"))
				search_query.append("s.guid_show = uuid_to_bin(%s)")
				search_params.append(flask.request.json.get("guid_show"))
			
			# Check for shot properties
			shot_properties = flask.request.json.get("shot")
			for param in ("shot","frm_start","frm_duration","frm_end"):
				if shot_properties.get(param) is not None:
					if param == "guid_shot":
						search_query.append("s.guid_shot = uuid_to_bin(%s)")

					elif param == "frm_start":
						search_query.append(f"s.frm_start {'<=' if subclip else '='} %s")
					
					elif param == "frm_end":
						search_query.append(f"s.frm_end {'>=' if subclip else '='} %s")

					elif param == "frm_duration":
						search_query.append(f"s.frm_duration {'>=' if subclip else '='} %s")
					
					else:
						search_query.append(f"s.{param} = %s")
					
					search_params.append(shot_properties.get(param))

			# Check for metadata
			if flask.request.json.get("metadata"):
				print("Searching for metadata", flask.request.json.get("metadata"))
				search_query.append("JSON_CONTAINS(m.extended_info, %s)")
				search_params.append(flask.json.dumps(flask.request.json.get("metadata")))
			
			print(tuple(search_query))
			print(tuple(search_params))
			
			if not len(search_query):
				results = []

			else:
				cur.execute(f"""
					SELECT
						bin_to_uuid(s.guid_shot) as guid_shot,
						bin_to_uuid(s.guid_show) as guid_show,
						s.shot as shot,
						s.frm_start as frm_start,
						s.frm_duration as frm_duration,
						s.frm_end as frm_end,
						CAST(s.frm_rate as CHAR) as frm_rate,
						IFNULL(m.extended_info, JSON_OBJECT()) as metadata
					FROM dailies_shots s
					LEFT JOIN dailies_metadata m ON m.guid_shot = s.guid_shot
					WHERE {' AND '.join(x for x in search_query)}
				""", tuple(search_params))
	#			print("Last query was:", cur._last_executed)
				results = cur.fetchall()
	
	return flask.jsonify(results)


@dailies.route("/v1/shots/<string:shot>/extended")
def list_shot_exteded(shot):

	cur = db.connection.cursor()	
	cur.execute("""
		SELECT extended_info
		FROM dailies_metadata
		WHERE guid_shot = uuid_to_bin(%s)
		LIMIT 1
	""", (shot,))
	results = cur.fetchone()
	
	if cur.rowcount:
		return results.get("extended_info")
	else:
		return flask.jsonify({})


# TODO: This should become a function that simply queues a restore.
# Actual restore should be handled by a Windows service
@dailies.route("/v1/shots/<string:guid_shot>/divarestore")
def diva_restore_shot(guid_shot):

	cur = db.connection.cursor()
	cur.execute("""
		SELECT
			d.object_name as object_name,
			c.tv_path as tv_path,
			c.category as category,
			c.src_dest as src_dest
		FROM 
			dailies_shots s
		INNER JOIN
			diva_config c ON c.guid_show = s.guid_show			
		INNER JOIN
			dailies_diva d ON d.guid_shot = s.guid_shot
		WHERE
			s.guid_shot = uuid_to_bin(%s)
		""", (guid_shot,))
		
	results = cur.fetchall()

	diva = upco_diva.Diva(manager_ip="192.168.20.220", manager_port=9000)

	if len(results) == 0:
		print("No results")
	
	# TODO: Cleanup restores[] vs returned json results[]
	
	restores = []
	restores_return = []

	for result in results:

		# Check if exists
		try:
			path_file = pathlib.Path(result.get("tv_path"), result.get("object_name") + ".mov")
			result.update({"file_path": str(path_file), "file_name": path_file.name, "already_exists": False})
			if path_file.is_file():
				print(f"Skipping {path_file.stem}: Already exists at {path_file.resolve()}")
				result.update({"already_exists":True})
				restores_return.append(result)
				continue

		except Exception as e:
			print(f"Error checking for {result.get('object_name')}, will restore again: {e}")

		restores.append(diva.restoreObject(result.get("object_name"), result.get("category"), destination=result.get("src_dest")))
		restores_return.append(result)

	print(restores)
	
	return flask.jsonify(restores_return)