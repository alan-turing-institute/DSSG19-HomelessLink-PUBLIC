while IFS= read -r line
do
  pip freeze | grep "$line" >> all_imports.txt
done < all_imports.txt
