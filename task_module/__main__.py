#!/usr/bin/env python3

import connexion

from task_module import encoder
from aihandler.ai import AIntel

def main():
    app = connexion.App(__name__, specification_dir='./swagger/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('swagger.yaml', arguments={'title': 'Task.ai API'}, pythonic_params=True)
    ai = AIntel()
    app.run(port=80)


if __name__ == '__main__':
    main()
