run: venv/requirements.timestamp
	venv/bin/publish

venv/requirements.timestamp: venv/venv.timestamp requirements.txt
	venv/bin/pip3 install -r requirements.txt
	venv/bin/pip3 install -e .
	touch $@

venv/venv.timestamp:
	python3 -m venv venv
	venv/bin/pip3 install pip==22.2.2
	touch $@

clean:
	rm -rf venv
