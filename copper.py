from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

import local_common as common
import dbwriter
from pathlib import Path

from prefect import task, Flow
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
def set_up_driver():
    return common.set_up_driver()

@task(cache_policy=NO_CACHE)
def connect(DRIVER):
    print("starting...")
    web = "https://www.coppercolorado.com/the-mountain/trail-lift-info/winter-trail-report"
    DRIVER.get(web)

    # open lift menus
    for button in DRIVER.find_elements(By.XPATH, '//span[@class="panel-icon"]'):
        button.click()

    # each row within all the tables
    runs = []
    lifts = []
    rows = DRIVER.find_elements(By.TAG_NAME, "tr")
    for run in rows:
        if common.isElementPresent(run, By.XPATH, ".//*[name()='td'][@class='type']"):
            # LIFTS
            lift_name, lift_status, lift_type = None, None, None
            lift_name = run.find_element(By.CLASS_NAME, "name").get_attribute("innerHTML")
            lift_status = common.isElementPresent(run, By.XPATH, ".//*[name()='path'][@fill='#8BC53F']")
            lift_type = run.find_element(By.CLASS_NAME, "type").get_attribute("innerHTML")
            
            lift_obj = {
                "liftName": lift_name,
                "liftType": lift_type,
                "liftStatus": lift_status,
            }
            lifts.append(lift_obj)

        else:
            # RUNS
            run_name, run_status, run_difficulty = None, None, None

            run_name = run.find_element(By.CLASS_NAME, "name").get_attribute("innerHTML")
            run_status = common.isElementPresent(run, By.XPATH, ".//*[name()='path'][@fill='#8BC53F']") 
            try:
                difficulty_text = run.find_element(By.CLASS_NAME, "difficulty").find_element(By.TAG_NAME, "div").get_attribute("class")
                if "icon difficulty-level-green" in difficulty_text: run_difficulty = "green"
                elif "icon difficulty-level-blue" in difficulty_text: run_difficulty = "blue"
                elif "icon difficulty-level-black-3" in difficulty_text: run_difficulty = "black3"
                elif "icon difficulty-level-black-2" in difficulty_text: run_difficulty = "black2"
                elif "icon difficulty-level-black" in difficulty_text: run_difficulty = "black1"
                
            except NoSuchElementException as e:
                print("unable to find difficulty")
            run_obj = {
                "runName": run_name,
                "runStatus": run_status,
                "runDifficulty": run_difficulty,
            }
            runs.append(run_obj)

    DRIVER.quit()
    return lifts, runs

@task
def export(lifts, runs, location):
    json_string = common.prepareForExport(lifts, runs, location)
    results = dbwriter.main(json_string)
    return results

@Flow
def main():
    DRIVER = set_up_driver()
    lifts, runs = connect(DRIVER)
    results = export(lifts, runs, "copper")
    print(results)
    print("Done")

if __name__ == "__main__":
    main()
    main.from_source(
        source=str(Path(__file__).parent),
        entrypoint="copper.py:main",
    ).deploy(
        name="corduroy-scraper-copper",
        work_pool_name="local-pool",
        cron="0 13 * 10-12,1-5 *"
    )