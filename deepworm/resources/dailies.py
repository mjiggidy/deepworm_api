# Actual stuff for reading/writing from database
from ..db import db
from flask_mysqldb import MySQLdb

def getShots(guid_show=None, guid_shot=None):
	""" Return a list of dicitonaries for given GUIDs """

	# Assemble optional GUIDs for WHERE clause
	# TODO: I think I'm overcomplicating things here
	params = ""
	if guid_shot is not None:
		params = "WHERE guid_shot = uuid_to_bin(%s) LIMIT 1"
	elif guid_show is not None:
		params = "Where guid_show = uuid_to_bin(%s)"

	print(f"Using params: {params} with args {tuple([guid_shot or guid_show])}")

	cur = db.connection.cursor()	
	cur.execute(f"""
		SELECT
			bin_to_uuid(guid_show) as guid_show,
			bin_to_uuid(guid_shot) as guid_shot,
			shot,
			frm_start,
			frm_duration,
			frm_end,
			frm_rate,
			metadata
		FROM view_shotinfo
		{params}
	""", tuple([guid_shot or guid_show]))
	
	return cur.fetchall()

def addSelect(guid_show, shot_name, frm_start, frm_end, selects_reel):
	"""Check in a new selected frame range"""
	
	# Try to insert
	cur = db.connection.cursor()
	
	try:
		cur.execute("""
			INSERT INTO
				dailies_selects(guid_show, guid_shot, selects_reel, frm_start, frm_duration)
			VALUES
				(uuid_to_bin(%(guid_show)s), (SELECT guid_shot FROM dailies_shots WHERE shot = %(shot_name)s AND frm_start <= %(frm_start)s AND frm_end >= %(frm_end)s), %(selects_reel)s, %(frm_start)s, %(frm_end)s-%(frm_start)s)
			""",
			{"guid_show":guid_show, "shot_name":shot_name, "frm_start":frm_start, "frm_end":frm_end, "selects_reel":selects_reel}
		)

	# It's probably okay if it's already in there...
	except MySQLdb._exceptions.IntegrityError as e:
		print("Didn't add new select: ", e)
		pass

	# ...but freak out for anything else
	except Exception as e:
		cur.close()
		raise e
	cur.close()

	# Return from view

	return searchSelects(guid_show=guid_show, shot_name=shot_name, frm_start=frm_start, frm_end=frm_end, strict=False)

def searchSelects(guid_show=None, guid_shot=None, shot_name=None, selects_reel=None, frm_start=None, frm_end=None, strict=True):
	"""Return select from a given frame range"""

	params = []
	values = []

	if guid_show is not None:
		params.append("guid_show = uuid_to_bin(%s)")
		values.append(guid_show)
	
	if selects_reel is not None:
		params.append("selects_reel = %s")
		values.append(selects_reel)
	
	if guid_shot is not None:
		params.append("guid_shot = uuid_to_bin(%s)")
		values.append(guid_shot)
	
	elif shot_name is not None:
		if strict:
			params.append("shot = %s")
			values.append(shot_name)

		else:
			params.append("shot LIKE %s")
			values.append(f"%{shot_name}%")
	
	if frm_start is not None and frm_end is not None:
		
		if strict:
			params.append("frm_start = %s AND frm_end = %s")
			values.extend([frm_start, frm_end])
		
		else:
			# Find any intersecting
			params.append("((%s<=frm_start AND %s >frm_start) OR (%s<frm_end AND %s >= frm_end))")
			values.extend([frm_start, frm_end, frm_start, frm_end])
	

	cur = db.connection.cursor()
	try:
		cur.execute(f"""
			SELECT
				*
			FROM
				view_selects
			WHERE
				{" AND ".join(params)}
			ORDER BY
				date_added
			DESC
		""", tuple(values))
		results = cur.fetchall()
	except Exception as e:
		cur.close()
		raise e

	print("Found", results)

	cur.close()
	return results

