from flask_script import Manager, Server

from app import create_application
from app.settings import settings

application = create_application()
manager = Manager(application)

server = Server(host='0.0.0.0', port=settings.PORT, use_debugger=settings.DEBUG)

manager.add_command('runserver', server)

if __name__ == '__main__':
    manager.run()
