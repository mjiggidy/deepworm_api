import pathlib
import flask, flask_mysqldb
from .db import db
from .blueprints.dailies import dailies

def create_app(test_config = None):
	app = flask.Flask(__name__, instance_relative_config=True)
	app.config.from_mapping(
		SECRET_KEY = "dev"
	)

	if test_config is None:
		app.config.from_pyfile("config.py")
	else:
		app.config.from_mapping(test_config)

	# Init DB
	db.init_app(app)


	# Register Blueprints
	app.register_blueprint(dailies)

	# Create instance path
	try:
		pathlib.Path(app.instance_path).mkdir(parents=True, exist_ok=True)
	except OSError:
		pass

	return app