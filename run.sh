#!/bin/bash

# Exécution manuelle des scripts dans "Data Science - 0"
for file in "Data Science - 0/ex02/table.py" "Data Science - 0/ex03/automatic_table.py" "Data Science - 0/ex04/items_table.py"; do
    # Vérifier si le fichier existe
    if [ -f "$file" ]; then
        dir=$(dirname "$file")
        echo "Changing directory to $dir"
        cd "$dir" || exit 1
        
        echo "Executing $file"
        python3 "$(basename "$file")" || exit 1
        echo "Done"
        
        # Demander à l'utilisateur s'il veut continuer
        read -p "Press Enter to execute the next script or Ctrl+C to exit."

        # Revenir au répertoire initial
        cd - > /dev/null || exit 1
    else
        echo "File $file not found!"
    fi
done

# Exécution manuelle des scripts dans "Data Science - 1"
for file in "Data Science - 1/ex01/customers_table.py" "Data Science - 1/ex02/remove_duplicates.py" "Data Science - 1/ex03/fusion.py" "Data Science - 1/ex04/another_script.py"; do
    # Vérifier si le fichier existe
    if [ -f "$file" ]; then
        dir=$(dirname "$file")
        echo "Changing directory to $dir"
        cd "$dir" || exit 1
        
        echo "Executing $file"
        python3 "$(basename "$file")" || exit 1
        echo "Done"
        
        # Demander à l'utilisateur s'il veut continuer
        read -p "Press Enter to execute the next script or Ctrl+C to exit."
        
        # Revenir au répertoire initial
        cd - > /dev/null || exit 1
    else
        echo "File $file not found!"
    fi
done
