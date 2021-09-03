
const ADL['verbs'] = require('verbs.min.js');
class BigQueryStatementBuilder {
  constructor() {}

  createXapiStatement (row) {
    let statement = {
      id: generateStatementId(row),
      actor: {
        objectType: "Agent",
        account: {
          "homePage": "https://curiouslearning.org",
          "name": row.user_pseudo_id,
        }
      },
     verb: getVerbObject(row),
     object: {
        objectType: selectObjectType(row),
        id: getObjectIri(row),
      },
        timestamp: row.event_timestamp,
    };
    statement = checkForAndAddResult(statement);
  }

  generateStatementId(row) {
      //TODO: determine method for generating statement uuid
  }

  getVerbObject(row) {
    //TODO: determine mapping of FTM data to ADLNet verbs
    /* Event to Verb mapping
    * SegmentFail: failed
    * LevelFail: failed
    * SegmentSuccess: completed
    * LevelSuccess: completed
    * Profile Select: interacted
    * Monster Select: interacted
    * SubSkill Increase: progressed
    * Monster Petting: interacted
    * Monster Petting Done: completed
    * New Monster: experienced
    * Monster evolve: progressed
    */
  }

  selectObjectType(row){
    /* Object Types:
    * Splash Screen
    * Profile
    * Screen
    * Map
    * Settings
    * Level
    * Segment
    * Monster
    * Puzzle Letter
    * Puzzle Sound Letter
    * Puzzle Letter In Word
    * Puzzle Word
    * Puzzle Sound Word
    * Letter Tracing
    * Parents Report
    * Memory Game
    * Monster Petting
    * SubSkill
    * Session
    */
    //TODO: determine scope of objects in BigQuery results
  }

  getObjectIri(row) {
    //TODO: build IRI library for FTM/CuriousReader objects
  }

  checkForAndAddResult(statement) {
    if(requiresResult(statement.verb.id)) {
      let result= {
        score: {
          scaled: ,
          raw: ,
          min: ,
          max: ,
        },
        success: ,
        completion: ,
        duration: ,
        response: ,
        duration: ,
      };
      statement['result'] = result;
    }
    return statement;
  }

}

module.exports = {
  BigQueryStatementBuilder,
};
