import os
import subprocess
import re

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def convert_ui_to_py(ui_dir, py_dir):
    os.makedirs(py_dir, exist_ok=True)

    for filename in os.listdir(ui_dir):
        if filename.endswith('.ui'):
            ui_file = os.path.join(ui_dir, filename)

            # Convert filename to snake_case
            py_filename = camel_to_snake(filename[:-3]) + '_ui.py'
            py_file = os.path.join(py_dir, py_filename)

            # Run pyuic5 command
            command = f'pyuic6 -x "{ui_file}" -o "{py_file}"'
            subprocess.run(command, shell=True, check=True)

            # Read the generated file
            with open(py_file, 'r') as file:
                content = file.read()

            # Extract the original class name
            match = re.search(r'class (Ui_\w+)', content)
            if match:
                original_class_name = match.group(1)
                # Generate the new class name
                new_class_name =  'Ui_'+''.join(word.capitalize() for word in py_filename[:-6].split('_'))

            # Write the modified content back to the file
            with open(py_file, 'w') as file:
                file.write(content)

            print(f"Converted {filename} to {py_filename} with class name {new_class_name}")


# Usage
ui_directory = '.'
py_output_directory = '.'

for folder in ['.']:
    convert_ui_to_py(folder, folder)