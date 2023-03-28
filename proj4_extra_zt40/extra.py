import sqlalchemy
import math

from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from sqlalchemy import Table, create_engine, MetaData, text, Column, Integer, String, Float, Sequence, ForeignKey, inspect
from sqlalchemy.orm import relationship, declarative_base, subqueryload
Base = declarative_base()

class COLOR(Base):
    __tablename__ = "color"
    
    COLOR_ID = Column(Integer, Sequence("COLOR_ID_SEQ", start=1, increment=1), primary_key = True)
    NAME = Column(String)
    
class STATE(Base):
    __tablename__ = "state"
    
    STATE_ID = Column(Integer, Sequence("STATE_ID_SEQ", start=1, increment=1), primary_key = True)
    NAME = Column(String)

class TEAM(Base):
    __tablename__ = "team"
    
    TEAM_ID = Column(Integer, Sequence("TEAM_ID_SEQ", start=1, increment=1), primary_key = True)
    NAME = Column(String)
    STATE_ID = Column(Integer, ForeignKey('state.STATE_ID'))
    COLOR_ID = Column(Integer, ForeignKey('color.COLOR_ID'))
    WINS = Column(Integer)
    LOSSES = Column(Integer)
    
    location = relationship("STATE", backref="state")
    coloring = relationship("COLOR", backref="color")
    
class PLAYER(Base):
    __tablename__ = "player"

    PLAYER_ID = Column(Integer, Sequence('PLAYER_ID_SEQ', start=1, increment=1), primary_key = True)
    TEAM_ID = Column(Integer, ForeignKey('team.TEAM_ID'))
    UNIFORM_NUM = Column(Integer)
    FIRST_NAME = Column(String)
    LAST_NAME = Column(String)
    MPG = Column(Integer)
    PPG = Column(Integer)
    RPG = Column(Integer)
    APG = Column(Integer)
    SPG = Column(Float)
    BPG = Column(Float)
    
    serves = relationship("TEAM", backref="team")

def query1(engine,  
           use_mpg,
           min_mpg,
           max_mpg,             
           use_ppg,             
           min_ppg,             
           max_ppg,             
           use_rpg,             
           min_rpg,             
           max_rpg,             
           use_apg,             
           min_apg,             
           max_apg,             
           use_spg,             
           min_spg,             
           max_spg,             
           use_bpg,             
           min_bpg,             
           max_bpg):
    print("PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG")
    Session = sessionmaker(bind=engine)
    session = Session()
    result = session.query(PLAYER)

    if (use_mpg):
        result = result.filter(PLAYER.MPG >= min_mpg).filter(PLAYER.MPG <= max_mpg)
    if (use_ppg):
        result = result.filter(PLAYER.PPG >= min_ppg).filter(PLAYER.PPG <= max_ppg)
    if (use_rpg):
        result = result.filter(PLAYER.RPG >= min_rpg).filter(PLAYER.RPG <= max_rpg)
    if (use_apg):
        result = result.filter(PLAYER.APG >= min_apg).filter(PLAYER.APG <= max_apg)        
    if (use_spg):
        result = result.filter(PLAYER.SPG >= min_spg).filter(PLAYER.SPG <= max_spg)
    if (use_bpg):
        result = result.filter(PLAYER.BPG >= min_bpg).filter(PLAYER.BPG <= max_bpg)
    for r in result:
        print(r.PLAYER_ID, r.TEAM_ID, r.UNIFORM_NUM, r.FIRST_NAME, r.LAST_NAME, r.MPG, r.PPG, r.RPG, r.APG, math.floor(r.SPG * 10)/10, math.floor(r.BPG * 10)/10)
    session.close()
    
def query2(engine, team_color):
    print("NAME")
    Session = sessionmaker(bind=engine)
    session = Session()   
    result = session.query(TEAM).join(COLOR).filter(COLOR.NAME == team_color).all()
    for r in result:
        print(r.NAME)
    session.close()

def query3(engine, team_name):
    print("FIRST_NAME LAST_NAME")
    Session = sessionmaker(bind=engine)
    session = Session()   
    result = session.query(PLAYER).join(TEAM).filter(TEAM.NAME == team_name).order_by(PLAYER.PPG.desc()).all()
    for r in result:
        print(r.FIRST_NAME, r.LAST_NAME)
    session.close()

def query4(engine, team_state, team_color):
    print("UNIFORM_NUM FIRST_NAME, LAST_NAME")
    Session = sessionmaker(bind=engine)
    session = Session()   
    result = session.query(PLAYER, TEAM, STATE, COLOR).filter(TEAM.TEAM_ID == PLAYER.TEAM_ID).filter(TEAM.COLOR_ID == COLOR.COLOR_ID).filter(TEAM.STATE_ID == STATE.STATE_ID).filter(STATE.NAME == team_state).filter(COLOR.NAME == team_color).all()
    for r in result:
        print(r.PLAYER.UNIFORM_NUM, r.PLAYER.FIRST_NAME, r.PLAYER.LAST_NAME)
    session.close()
    
def query5(engine, num_wins):
    print("FIRST_NAME LAST_NAME NAME WINS")
    Session = sessionmaker(bind=engine)
    session = Session()   
    result = session.query(PLAYER, TEAM).join(TEAM).filter(TEAM.WINS >= num_wins).all()
    for r in result:
        print(r.PLAYER.FIRST_NAME, r.PLAYER.LAST_NAME, r.TEAM.NAME, r.TEAM.WINS)
    session.close()

def add_player(engine, team_id, jersey_num, first_name, last_name, mpg, ppg, rpg, apg, spg, bpg):
    entry = PLAYER(TEAM_ID=team_id,                   
                   UNIFORM_NUM=jersey_num,  
                   FIRST_NAME=first_name, 
                   LAST_NAME=last_name, 
                   MPG=mpg,
                   PPG=ppg,
                   RPG=rpg,
                   APG=apg,
                   SPG=spg,
                   BPG=bpg)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add(entry)
    session.commit()
    session.close()
    
def add_color(engine, name):
    entry = COLOR(NAME=name)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add(entry)    
    session.commit()
    session.close()

def add_state(engine, name):
    entry = STATE(NAME=name)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add(entry)    
    session.commit()
    session.close()    

def add_team(engine, name, state_id, color_id, wins, losses):
    entry = TEAM(NAME=name, STATE_ID=state_id, COLOR_ID=color_id, WINS=wins, LOSSES=losses)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add(entry)    
    session.commit()
    session.close()        

def exerciser(engine):
    add_player(engine, 1, 100, "A", "B", 1, 1, 1, 1, 1.56, 17.89)
    query1(engine, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
    query2(engine, "Maroon")
    query3(engine, "UNC")
    query5(engine, 10)
    query4(engine, "MA", "Maroon")
 
    return

if __name__ == "__main__":
    engine = create_engine(URL.create(username = 'postgres',
                                  password = 'passw0rd',
                                  database = 'ACC_BBALL',
                                  drivername = 'postgresql'))    
    Base.metadata.drop_all(engine, checkfirst=True)
    Base.metadata.create_all(engine)


    Session = sessionmaker(bind=engine)
    session = Session()
    f = open("color.txt", "r")
    for l in f:
        args = l.split(" ")
        entry = COLOR( NAME=(args[1][:-1]))
        session.add(entry)    
        session.commit()

    f.close()

    f = open("state.txt", "r")
    for l in f:
        args = l.split(" ")
        entry = STATE(NAME=(args[1][:-1]))
        session.add(entry)    
        session.commit()

    f.close()

    f = open("team.txt", "r")
    for l in f:
        args = l.split(" ")
        entry = TEAM(NAME=(args[1]), STATE_ID=int(args[2]), COLOR_ID=int(args[3]), WINS=int(args[4]), LOSSES=int(args[5]))
        session.add(entry)    
        session.commit()

    f.close()

    f = open("player.txt", "r")
    for l in f:
        args = l.split(" ")
        entry = PLAYER(
                    TEAM_ID=int(args[1]),
                    UNIFORM_NUM=int(args[2]),
                    FIRST_NAME=(args[3]), 
                    LAST_NAME=(args[4]), 
                    MPG=int(args[5]),
                    PPG=int(args[6]),
                    RPG=int(args[7]),
                    APG=int(args[8]),
                    SPG=float(args[9]),
                    BPG=float(args[10])
                    )

        session.add(entry)    
        session.commit()
    f.close()
    exerciser(engine)
