# Function to generate a folder name with the last day of the month
def generate_folder_name():
    # Get the current date
    current_date = datetime.now().date()
    # Get the last day of the current month
    last_day_of_month = calendar.monthrange(current_date.year, current_date.month)[1]
    # Create a new date with the last day of the current month
    last_day_date = datetime(current_date.year, current_date.month, last_day_of_month)
    # Format the date as YYYY-MM-DD
    formatted_date = last_day_date.strftime("%Y-%m-%d")
    # Generate the folder name with the formatted date
    folder_name = f"report_{formatted_date}"
    return folder_name

# Example usage:
folder_name = generate_folder_name()
