import flask
import subprocess, pathlib, tempfile, sys
from upco_tools import upco_ale, upco_timecode
from ..db import db


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

	cur = db.connection.cursor()	
	cur.execute("""
		SELECT
			bin_to_uuid(s.guid_shot) as guid_shot,
			s.shot as shot,
			s.frm_start as frm_start,
			s.frm_duration as frm_duration,
			s.frm_end as frm_end,
			IFNULL(m.extended_info, JSON_OBJECT()) as metadata
		FROM dailies_shots s
		LEFT JOIN dailies_metadata m ON m.guid_shot = s.guid_shot
		WHERE s.guid_show = uuid_to_bin(%s)
	""", (show,))
	results = cur.fetchall()
	
	return flask.jsonify(results)

# NOTE: Recent change, may break some things
@dailies.route("/v1/shots/<string:guid>/")
def get_shot_by_guid(guid):

	cur = db.connection.cursor()	
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
	""", (guid,))
	results = cur.fetchone()
	
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

		if flask.request.form.get("guid_shot") is not None:
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
			metadata = flask.request.json.get("metadata")
			search = set()
			for param in ("guid_show","shot","frm_start","frm_duration"):
				if metadata.get(param) is not None:
					if param == "guid_show":
						search.add(f"s.guid_show = uuid_to_bin(%s)")
					else:
						search.add(f"s.{param} = %s")
			if not len(search):
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
						IFNULL(m.extended_info, JSON_OBJECT()) as metadata
					FROM dailies_shots s
					LEFT JOIN dailies_metadata m ON m.guid_shot = s.guid_shot
					WHERE {' AND '.join(x for x in search)}
				""", (metadata.get(x) for x in metadata.keys()))
				print(cur._last_executed)
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
		
@dailies.route("/v1/diva/restore", methods=["POST"])
def diva_restore_request():

	if not flask.request.is_json:
		return flask.jsonify({"error_message": "Must be a JSON request."}, 400)
	
	diva_request_succ = []
	diva_request_errs = []
	
	for shot in flask.request.json:
		print(f"Restoring {shot}")
		cur = db.connection.cursor()
		cur.execute("""
			SELECT
				bin_to_uuid(dailies_diva.guid_shot) as guid_shot,
				bin_to_uuid(dailies_diva.guid_show) as guid_show,
				dailies_diva.object_name as object_name,
				diva_config.category as category,
				diva_config.src_dest as src_dest
			FROM
				dailies_diva, diva_config
			WHERE
				diva_config.guid_show= dailies_diva.guid_show AND dailies_diva.guid_shot = uuid_to_bin(%s)
		""", (shot.get("guid_shot"),))
		
		for diva_object in cur.fetchall():
#			diva_request_ids.append(diva_object.get("object_name"))
			proc_diva_restore = subprocess.Popen([r"C:\Users\LTOtransfer\Desktop\DivaScript\Utilities\bin\DivaScript.exe",
				"restore",
				"-obj", diva_object.get("object_name"),
				"-cat", diva_object.get("category"),
				"-src", diva_object.get("src_dest")],
				text=True,
				universal_newlines=True,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			
			try:
				pipe_out, pipe_err = proc_diva_restore.communicate(timeout=10)
			except Exception as e:
				print("Retryin...")
				proc_diva_restore.kill()
				pipe_out, pipe_err = proc_diva_restore.communicate(timeout=10)
			
			if pipe_out.strip().isnumeric() and not pipe_err:
				diva_object.update({"req_id": pipe_out.strip()})
				diva_request_succ.append(diva_object)
				
				
		
	for diva_req in diva_request_succ:
		print(diva_req)
		thing = cur.execute("""
			INSERT INTO diva_requests(req_id, guid_shot, type, status, location)
			VALUES (
				%s,
				uuid_to_bin(%s),
				%s,
				%s,
				%s
			)""", (diva_req.get("req_id"), diva_req.get("guid_shot"), "Restore","Restoring...",diva_req.get("src_dest"),))
		

#		print(f"Shot {diva_object.get('object_name')}:\tOut: {pipe_out}\tErr:{pipe_err}")
		
	#print(diva_request_ids)
		
	return "Done"
		
	
			
		
@dailies.route("/dailies/v1/metadata/ale", methods=["POST"])
def ale_from_shots():

	#print(f"Is Json? {flask.request.is_json}")
	#print(f"Received data: {flask.request.json}")
	
	ale = upco_ale.Ale()
	for shot in flask.request.json:
		cur = db.connection.cursor()
		cur.execute("""
			SELECT 
				bin_to_uuid(dailies_shots.guid_shot) as guid_shot,
				shot,
				frm_start,
				frm_end,
				extended_info,
				(SELECT object_name FROM dailies_diva WHERE dailies_diva.guid_shot = dailies_shots.guid_shot LIMIT 1) as diva_name,
				(SELECT proxy_name FROM dailies_proxies WHERE dailies_proxies.guid_shot = dailies_shots.guid_shot LIMIT 1) as proxy_name
			FROM dailies_shots 
			LEFT JOIN dailies_metadata ON dailies_metadata.guid_shot = dailies_shots.guid_shot 
			WHERE dailies_shots.guid_shot = uuid_to_bin(%s)
		""",(shot.get("guid_shot"),))

		shot_data = cur.fetchone()
		
		if shot_data.get("proxy_name"): clip_name = shot_data.get("proxy_name")
		elif shot_data.get("diva_name"): clip_name = shot_data.get("diva_name")
		else: clip_name = shot.get("shot")
		
		# TODO: Get Scene/Take/Camroll info from extended_info json
		shot_masterclip = {
			
			"Name": str(clip_name),
			"Tape": str(shot_data.get("shot")),
			"Start": str(upco_timecode.Timecode(shot_data.get("frm_start"))),
			"End": str(upco_timecode.Timecode(shot_data.get("frm_end"))),
			"Scene": str(shot_data.get("scene","?")),
			"Take": shot_data.get("take","?"),
			"Camroll": str(shot_data.get("camroll","?"))
		}
		
		# Unpack and merge extended_info
		if shot_data.get("extended_info"): shot_masterclip.update(flask.json.loads(shot_data.get("extended_info")))
		
		# Remove problematic columns (Color, Tracks, etc) that confuse Avid if not present for all shots
		removed = [shot_masterclip.pop(x) for x in ("Color","Tracks","Creation Date","Duration","Video","Videoframerate") if x in shot_masterclip.keys()]
		
		print(f"Removed {removed}")
		
		ale.addClips([shot_masterclip])
		
	ale_data = str()
	
	# Current upco_ale library only writes to a file.  My lack of foresight continues to astonish.
	# For now, write the ALE to a temp file, then read the file back into memory.
	with tempfile.TemporaryDirectory() as dir_temp:
		
		path_ale = pathlib.Path(dir_temp, "export.ale")
		ale.writeAle(path_ale)
		with path_ale.open("r", newline="\r\n", encoding="utf-8") as exported_ale:
			ale_data = str().join(exported_ale.readlines())
	
	return str(ale_data)

