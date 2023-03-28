#include "query_funcs.h"

using namespace std;
void add_player(connection * C,
                int team_id,
                int jersey_num,
                string first_name,
                string last_name,
                int mpg,
                int ppg,
                int rpg,
                int apg,
                double spg,
                double bpg) {
  stringstream q;
  q << "INSERT INTO PLAYER (TEAM_ID, UNIFORM_NUM, FIRST_NAME, LAST_NAME, MPG, PPG, RPG, "
       "APG, SPG, BPG) VALUES (";
  q << team_id << ", " << jersey_num << ", " << check_special(C, first_name) << ", "
    << check_special(C, last_name) << ", " << mpg << ", " << ppg << ", " << rpg << ", "
    << apg << ", " << spg << ", " << bpg << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();
}

void add_player(connection * C, vector<string> params) {
  add_player(C,
             stoi(params[1]),
             stoi(params[2]),
             params[3],
             params[4],
             stoi(params[5]),
             stoi(params[6]),
             stoi(params[7]),
             stoi(params[8]),
             stod(params[9]),
             stod(params[10]));
  return;
  /*
  stringstream q;
  q << "INSERT INTO PLAYER (TEAM_ID, UNIFORM_NUM, FIRST_NAME, LAST_NAME, MPG, "
       "PPG, RPG, "
       "APG, SPG, BPG) VALUES (";
  q << stoi(params[1]) << ", " << stoi(params[2]) << ", "
    << check_special(C, params[3]) << ", " << check_special(C, params[4]) << ", "
    << stoi(params[5]) << ", " << stoi(params[6]) << ", " << stoi(params[7]) << ", "
    << stoi(params[8]) << ", " << stod(params[9]) << ", " << stod(params[10]) << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();
  */
}

void add_team(connection * C,
              string name,
              int state_id,
              int color_id,
              int wins,
              int losses) {
  stringstream q;
  q << "INSERT INTO TEAM (NAME, STATE_ID, COLOR_ID, WINS, LOSSES) VALUES ("
    << check_special(C, name) << ", " << state_id << ", " << color_id << ", " << wins
    << ", " << losses << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();
}
void add_team(connection * C, vector<string> params) {
  add_team(
      C, params[1], stoi(params[2]), stoi(params[3]), stoi(params[4]), stoi(params[5]));
  return;
  /*
  stringstream q;
  q << "INSERT INTO TEAM (NAME, STATE_ID, COLOR_ID, WINS, LOSSES) VALUES ("
    << check_special(C, params[1]) << ", " << stoi(params[2])
    << ", " << stoi(params[3]) << ", " << stoi(params[4]) << ", " << stoi(params[5])
    << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();*/
}
void add_state(connection * C, string name) {
  stringstream q;
  q << "INSERT INTO STATE (NAME) VALUES(" << check_special(C, name) << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();
}
void add_state(connection * C, vector<string> params) {
  add_state(C, params[1]);
  return;
  /*
  stringstream q;
  q << "INSERT INTO STATE (NAME) VALUES(" 
    << check_special(C, params[1]) << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();
*/
}
void add_color(connection * C, string name) {
  stringstream q;
  q << "INSERT INTO COLOR (NAME) VALUES(" << check_special(C, name) << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();
}

void add_color(connection * C, vector<string> params) {
  add_color(C, params[1]);
  return;
  /*
  stringstream q;
  q << "INSERT INTO COLOR (NAME) VALUES(" 
    << check_special(C, params[1]) << ");";
  work W(*C);
  W.exec(q.str());
  W.commit();
  */
}
vector<string> split(string target) {
  stringstream ss(target);
  vector<string> res;
  string s;
  while (getline(ss, s, ' ')) {
    res.push_back(s);
  }
  return res;
}
string check_special(connection * C, string in) {
  work W(*C);
  string res = W.quote(in);
  W.commit();
  return res;
}

void print_res(result res) {
  for (auto row = res.begin(); row != res.end(); row++) {
    for (auto field = row.begin(); field != row.end(); field++)
      cout << field->c_str() << ' ';
    cout << endl;
  }
}
/*
 * All use_ params are used as flags for corresponding attributes
 * a 1 for a use_ param means this attribute is enabled (i.e. a WHERE clause is needed)
 * a 0 for a use_ param means this attribute is disabled
 */
void query1(connection * C,
            int use_mpg,
            int min_mpg,
            int max_mpg,
            int use_ppg,
            int min_ppg,
            int max_ppg,
            int use_rpg,
            int min_rpg,
            int max_rpg,
            int use_apg,
            int min_apg,
            int max_apg,
            int use_spg,
            double min_spg,
            double max_spg,
            int use_bpg,
            double min_bpg,
            double max_bpg) {
  stringstream q;
  nontransaction N(*C);
  cout << "PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG\n";
  q << "SELECT PLAYER_ID, TEAM_ID, UNIFORM_NUM, FIRST_NAME, LAST_NAME, MPG, "
       "PPG, RPG, APG, TRUNC(CAST(SPG AS NUMERIC), 1), TRUNC(CAST(BPG AS NUMERIC),1) "
       "FROM "
       "PLAYER";
  q << " WHERE ";
  if (use_mpg || use_ppg || use_rpg || use_apg || use_spg || use_bpg) {
    if (use_mpg) {
      q << "MPG >= " << min_mpg << " AND MPG <= " << max_mpg << " AND ";
    }
    if (use_ppg) {
      q << "PPG >= " << min_ppg << " AND PPG <= " << max_ppg << " AND ";
    }
    if (use_rpg) {
      q << "RPG >= " << min_rpg << " AND RPG <= " << max_rpg << " AND ";
    }
    if (use_apg) {
      q << "APG >= " << min_apg << " AND APG <= " << max_apg << " AND ";
    }
    if (use_spg) {
      q << "SPG >= " << min_spg << " AND SPG <= " << max_spg << " AND ";
    }
    if (use_bpg) {
      q << "BPG >= " << min_bpg << " AND BPG <= " << max_bpg << " AND ";
    }
  }
  q << "PLAYER_ID = PLAYER_ID;";
  result res = N.exec(q.str());
  print_res(res);
  N.commit();
}

void query2(connection * C, string team_color) {
  string cleaned = check_special(C, team_color);
  cout << "NAME\n";
  nontransaction N(*C);

  result res = N.exec("SELECT TEAM.NAME FROM TEAM, COLOR WHERE TEAM.COLOR_ID = "
                      "COLOR.COLOR_ID AND COLOR.NAME = " +
                      cleaned + ";");

  print_res(res);
  N.commit();
}

void query3(connection * C, string team_name) {
  string cleaned = check_special(C, team_name);
  cout << "FIRST_NAME LAST_NAME\n";
  nontransaction N(*C);

  result res = N.exec(
      "SELECT PLAYER.FIRST_NAME, PLAYER.LAST_NAME FROM PLAYER, TEAM WHERE TEAM.TEAM_ID = "
      "PLAYER.TEAM_ID AND TEAM.NAME = " +
      cleaned + "ORDER BY PLAYER.PPG DESC;");

  print_res(res);
  N.commit();
}

void query4(connection * C, string team_state, string team_color) {
  string cleaned_state = check_special(C, team_state);
  string cleaned_color = check_special(C, team_color);
  cout << "UNIFORM_NUM FIRST_NAME LAST_NAME\n";
  nontransaction N(*C);
  result res =
      N.exec("SELECT UNIFORM_NUM, FIRST_NAME, LAST_NAME FROM PLAYER,TEAM, "
             "STATE, COLOR WHERE PLAYER.TEAM_ID = TEAM.TEAM_ID AND TEAM.COLOR_ID = "
             "COLOR.COLOR_ID AND TEAM.STATE_ID = STATE.STATE_ID AND STATE.NAME = " +
             cleaned_state + " AND COLOR.NAME = " + cleaned_color + ";");
  print_res(res);
  N.commit();
}

void query5(connection * C, int num_wins) {
  cout << "FIRST_NAME LAST_NAME NAME WINS\n";
  nontransaction N(*C);
  stringstream ss;
  ss << num_wins;

  result res = N.exec("SELECT FIRST_NAME, LAST_NAME, TEAM.NAME, TEAM.WINS FROM PLAYER, "
                      "TEAM WHERE PLAYER.TEAM_ID = TEAM.TEAM_ID AND TEAM.WINS > " +
                      ss.str() + ";");
  print_res(res);
  N.commit();
}
