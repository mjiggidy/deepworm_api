# Actual stuff for reading/writing from database
from ..db import db

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