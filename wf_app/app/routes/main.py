from flask import Blueprint, render_template

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """Dashboard/Home page."""
    return render_template('index.html')


@main_bp.route('/dashboard')
def dashboard():
    """Main dashboard."""
    return render_template('dashboard.html') 