SELECT 
    dd.tradename,
    dd.activeingredient,
    aie.ingredient_name as matched_ingredient
FROM drug_ingredients di
JOIN drug_database dd ON di.drug_id = dd.drug_id
JOIN active_ingredients_extended aie ON di.ingredient_id = aie.id
ORDER BY dd.tradename;