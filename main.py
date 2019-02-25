# Oskar Sobczyk
# Uruchamianie aplikacji main.py dane.json
# Program zawiera zakomentowane linie. Sa to dodatkowe statusy pomagajace w debugowaniu

#Aktualizacja 12.06.18
# Poprawilem literowke w new(). Przekazywale zle haslo podczas tworzenia uzytkownika (haslo admina zamiast nowego uzytkownika)

import psycopg2
import json
import sys


class DBInterface:

    def __init__(self, login, pwd, dbname):
        try:
            self.connection = psycopg2.connect(
                user=login, password=pwd, dbname=dbname)
            self.curr = self.connection.cursor()
        except Exception as e:
            raise

    def authenticate(self, id, pwd):
        querry = "SELECT id FROM workers WHERE id = %s AND Password = crypt(%s, password)"

        try:
            self.curr.execute(querry, (id, pwd,))

            if(self.curr.fetchone()):
                return True
            return False
        except:
            return False

    def authenticate_hiearchy(self, id_admin, pwd, id_worker):
        querry_search = "SELECT Superior FROM workers WHERE ID = %s"
        querry_autchenticate = querry = "SELECT id FROM workers WHERE id = %s AND Password = crypt(%s, password)"

        res = id_worker

        try:
            while(res is not None and res != id_admin):
                self.curr.execute(querry_search, (res,))
                res = self.curr.fetchone()
                if (res is not None):
                    res = res[0]
                else:
                    return False

            self.curr.execute(querry_autchenticate, (res, pwd,))
            if (self.curr.fetchone()):
                return True
            else:
                return False

        except Exception as e:
            return False

    def initialize(self):

        f = open('init_table.sql', 'r').read()
        queries = f.split(';')

        for querry in queries[:-1]:
            try:
                self.curr.execute(querry)
            except:
                pass

        self.connection.commit()

    def root(self, data):
        querry = "INSERT INTO workers(ID, Password, Data) VALUES(%s,crypt(%s, gen_salt('bf', 8)),%s)"

        secret = data["secret"]
        pwd = data["newpassword"]
        d = data["data"]
        id = data["emp"]

        if(secret == "qwerty"):
            try:
                self.curr.execute(querry, (id, pwd, d,))
                ok = {"status": "OK", "debug": "root created"}
                print(json.dumps(ok))
            except Exception as e:
                error = {"status": "ERROR",
                         "debug": "problem with root creation"}
                print(json.dumps(error))
        else:
            error = {"status": "ERROR", "debug": "wrong secret"}
            print(json.dumps(error))
        self.connection.commit()

    def new(self, data):
        querry = "INSERT INTO workers(ID, Password, Data, Superior) VALUES(%s,crypt(%s, gen_salt('bf', 8)),%s,%s)"

        admin = data["admin"]
        pwd = data["passwd"]
        d = data["data"]
        npwd = data["newpasswd"]
        id_superior = data["emp1"]
        id = data["emp"]

        if(self.authenticate_hiearchy(admin, pwd, id_superior)):
            try:
                self.curr.execute(querry, (id, npwd, d, id_superior,))
                # ok = {"status": "OK",
                #       "debug": "created worker id {0}".format(id)}
                # print(json.dumps(ok))
            except Exception as e:
                # error = {"status": "ERROR",
                #          "debug": "worker creation failed id {0}".format(id)}
                # print(json.dumps(error))
                pass

            self.connection.commit()
        else:
            # error = {"status": "ERROR", "debug": "Authentication problem"}
            # print(json.dumps(error))
            pass

    def remove(self, data):
        querry = "DELETE FROM workers WHERE ID = %s"
        querry_parent = "SELECT Superior FROM workers WHERE ID = %s"
        admin = data["admin"]
        pwd = data["passwd"]
        id = data["emp"]
        parent = None

        try:
            self.curr.execute(querry_parent, (id,))
            res = self.curr.fetchall()
            if (res != []):
                parent = res[0][0]

            if(parent is not None and self.authenticate_hiearchy(admin, pwd, parent)):
                self.curr.execute(querry, (id,))
                self.connection.commit()
                # ok = {"status": "OK",
                #       "debug": "remove worker id {0}".format(id)}
                # print(json.dumps(ok))
            else:
                # error = {"status": "ERROR", "debug": "Authentication problem"}
                # print(json.dumps(error))
                pass
        except:
            error = {"status": "ERROR",
                     "debug": "Remove failed id {0}".format(id)}
            print(json.dumps(error))

    def child(self, data):
        querry = "SELECT ID FROM workers WHERE Superior = %s"

        id = data["emp"]
        admin = data["admin"]
        pwd = data["passwd"]

        try:
            if(self.authenticate(admin, pwd)):
                self.curr.execute(querry, (id,))
                childrens = [r[0] for r in self.curr.fetchall()]
                ok = {"status": "OK", "data": childrens,
                      "debug": "childrens for id {0}".format(id)}
                print(json.dumps(ok))
            else:
                error = {"status": "ERROR", "debug": "Authentication problem"}
                print(json.dumps(error))

        except Exception as e:
            error = {"status": "ERROR",
                     "debug": "Children operation failed id {0}".format(id)}
            print(json.dumps(error))

    def parent(self, data):
        querry = "SELECT Superior FROM workers WHERE ID = %s"

        id = data["emp"]
        admin = data["admin"]
        pwd = data["passwd"]

        try:
            if(self.authenticate(admin, pwd)):
                self.curr.execute(querry, (id,))
                res = self.curr.fetchall()
                if (res == []):
                    error = {"status": "ERROR"}
                    print(json.dumps(error))
                else:
                    parent = res[0][0]
                    ok = {"status": "OK", "data": [parent],
                          "debug": "Parent for id {0}".format(id)}
                    print(json.dumps(ok))
            else:
                error = {"status": "ERROR", "debug": "Authentication problem"}
                print(json.dumps(error))

        except Exception as e:
            error = {"status": "ERROR"}
            print(json.dumps(error))

    def update(self, data):

        querry = "UPDATE workers SET Data = %s WHERE ID = %s"

        id = data["emp"]
        admin = data["admin"]
        pwd = data["passwd"]
        newdata = data["newdata"]
        try:
            if(self.authenticate_hiearchy(admin, pwd, id)):
                self.curr.execute(querry, (newdata, id,))
                # ok = {"status": "OK",
                #       "debug": "Update information for id {0}".format(id)}
                # print(json.dumps(ok))
            else:
                # error = {"status": "ERROR", "debug": "Authentication problem"}
                # print(json.dumps(error))

                pass
        except Exception as e:
            # error = {"status": "ERROR",
            #          "debug": "Update failed for id {0}".format(id)}
            # print(json.dumps(error))
            pass

        self.connection.commit()

    def read(self, data):
        querry = "SELECT Data FROM workers WHERE ID = %s"

        id = data["emp"]
        admin = data["admin"]
        pwd = data["passwd"]

        try:
            if(self.authenticate_hiearchy(admin, pwd, id)):
                self.curr.execute(querry, (id,))
                data = self.curr.fetchall()
                if (data != []):
                    data = data[0][0]
                    ok = {"status": "OK", "data": data,
                          "debug": "data for id {0}".format(id)}
                    print(json.dumps(ok))
            else:
                error = {"status": "ERROR", "debug": "Authentication problem"}
                print(json.dumps(error))
        except Exception as e:
            error = {"status": "ERROR",
                     "debug": "read data failed id {0}".format(id)}
            print(json.dumps(error))

    def descendants(self, data):
        querry = "SELECT ID FROM workers WHERE Superior = %s"

        id = data["emp"]
        admin = data["admin"]
        pwd = data["passwd"]
        queue = [data["emp"]]
        data = []

        if(self.authenticate(admin, pwd)):

            try:
                while(len(queue) != 0):
                    search_id = queue[0]
                    queue = queue[1:]
                    self.curr.execute(querry, (search_id,))
                    childrens = [r[0] for r in self.curr.fetchall()]
                    queue += childrens
                    data += childrens

                output = {"status": "OK", "data": data,
                          "debug": "descendants for id {0}".format(id)}
                print(json.dumps(output))

            except Exception as e:
                error = {"status": "ERROR", "debug": "descendants error"}
                print(json.dumps(error))

        else:
            error = {"status": "ERROR", "debug": "Authentication problem"}
            print(json.dumps(error))

    def ancestors(self, data):
        querry = "SELECT Superior FROM workers WHERE ID = %s"

        id = data["emp"]
        admin = data["admin"]
        pwd = data["passwd"]

        data = []

        try:
            if(self.authenticate(admin, pwd)):
                self.curr.execute(querry, (id,))
                res = self.curr.fetchall()
                if (res != []):
                    res = res[0][0]
                    while(res is not None):
                        self.curr.execute(querry, (res,))
                        data.append(res)
                        res = self.curr.fetchall()[0][0]
                    ok = {"status": "OK", "data": data,
                          "debug": "ancesotrs for id {0}".format(id)}
                    print(json.dumps(ok))
            else:
                error = {"status": "ERROR", "debug": "Authentication problem"}
                print(json.dumps(error))

        except Exception as e:
            error = {"status": "ERROR", "debug": "ancestors error"}
            print(json.dumps(error))

    def ancestor(self, data):
        querry = "SELECT Superior FROM workers WHERE ID = %s"

        admin = data["admin"]
        pwd = data["passwd"]
        id1 = data["emp1"]
        id2 = data["emp2"]
        ok = {"status": "OK", "data": [False]}

        try:
            if(self.authenticate(admin, pwd)):
                self.curr.execute(querry, (id2,))
                res = self.curr.fetchall()
                if(res != []):
                    res = res[0][0]
                    while(res is not None):
                        self.curr.execute(querry, (res,))
                        if(res == id1):
                            ok = {"status": "OK", "data": [True]}
                            break
                        res = self.curr.fetchall()[0][0]

                print(json.dumps(ok))

            else:
                error = {"status": "ERROR", "debug": "Authentication problem"}
                print(json.dumps(error))

        except Exception as e:
            error = {"status": "ERROR", "debug": "ancestor error"}
            print(json.dumps(error))

    def close(self):
        self.connection.close()


class JsonInterpreter:
    def __init__(self, file):
        try:
            self.file = open(file)
        except:
            print("Json file problem")
            raise

        x = json.loads(self.file.readline())
        connection_info = x["open"]

        try:
            self.db = DBInterface(
                connection_info["login"], connection_info["password"], connection_info["database"])
            if(connection_info["login"] == "init"):
                self.db.initialize()
            ok = {"status": "OK", "debug": "connected"}
            print(json.dumps(ok))
        except Exception as e:
            error = {"status": "ERROR", "debug": "DB conection problem"}
            print(json.dumps(error))
            raise

    def execute(self):
        for line in self.file:
            json_data = json.loads(line)
            func = list(json_data.keys())[0]
            data = json_data[func]
            getattr(self.db, func)(data)

    def close(self):
        self.db.close()


def main():
    if (len(sys.argv) == 2):
        file = sys.argv[1]
        try:
            x = JsonInterpreter(file)
            x.execute()
            x.close()
        except:
            pass
    else:
        print("Wrong argument number")


if __name__ == "__main__":
    main()
