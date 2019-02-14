from app import create_application, settings
app = create_application()

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=settings.settings.get('PORT', 12000),
        debug=settings.settings.get('DEBUG', True))
