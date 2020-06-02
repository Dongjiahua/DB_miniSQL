import math


class Index:
    def __init__(self, n, root, attribute):
        self.n = n
        self.root = root
        self.attribute = attribute


class Node:
    def __init__(self, n, is_leaf):
        self.n = n
        self.parent = None
        self.keys = []
        self.pointers = []  # this stores other nodes for non-leaf nodes and
        # record address for leaf nodes(nodes for last one)
        self.is_leaf = is_leaf
        i = 0
        while i < n:
            self.pointers.append(None)
            i = i + 1

    def erase(self):  # except self.pointers[n - 1]
        i = 0
        l = len(self.keys)
        while i < l:
            self.keys.pop()
            self.pointers[i] = None
            i = i + 1

    def erase_all(self):
        i = 0
        l1 = len(self.pointers)
        l2 = len(self.keys)
        while i < l1:
            if i < l2:
                self.keys.pop()
            self.pointers[i] = None
            i = i + 1

    def is_root(self):
        if self.parent is None:
            return True
        else:
            return False

    def pointers_number(self):
        number = 0
        i = 0
        while i < len(self.pointers):
            if self.pointers[i] is not None:
                number = number + 1
            i = i + 1
        return number


# Returns leaf node Current and index i such that Current.Pi points to first record * with search key value V
def find(value, Tree):
    current = Tree.root
    while not current.is_leaf:
        i = 0
        while i < len(current.keys):
            if value > current.keys[i]:  # smallest greater
                i = i + 1
            else:
                break
        if i == len(current.keys):  # no such i
            # find the lase non-null pointer in the node
            k = len(current.pointers) - 1
            while k >= 0:
                if current.pointers[k] is not None:
                    break
                k = k - 1
            current = current.pointers[k]  # last pointer
        elif value == current.keys[i]:
            current = current.pointers[i + 1]
        else:
            current = current.pointers[i]  # value < current.key[i]
    # current is a leaf node
    i = 0
    while i < len(current.keys):
        if value != current.keys[i]:
            i = i + 1
        else:
            break
    if i != len(current.keys):  # exists
        return current, i
    else:
        return None, -1


def recordAll(current, i, value):
    done = False
    record = []
    if current is None:
        return None
    else:
        while not done and current is not None:
            while i < len(current.keys) and current.keys[i] <= value:
                record.append(current.pointers[i])
                i = i + 1
            if i >= len(current.keys):
                current = current.pointers[i]
                i = 0
            else:
                done = True
    return record


def insert_in_leaf(current, value, pointer, Tree):
    i = 0
    while i < len(current.keys):
        if current.keys[i] < value:
            i = i + 1
        else:
            break
    current.keys.insert(i, value)
    # because current.pointers have been initialized, so there is difference
    current.pointers.insert(i, pointer)
    if current.n == Tree.n:
        current.pointers[Tree.n - 1] = current.pointers[Tree.n]
        current.pointers.pop()
    else:
        current.pointers.pop()


def insert_in_parent(n, value, N, Tree):
    if Tree.root == n:
        R = Node(Tree.n, False)
        R.pointers.insert(0, n)
        R.keys.append(value)
        R.pointers.insert(1, N)
        R.pointers.pop()
        R.pointers.pop()
        n.parent = R
        N.parent = R
        Tree.root = R
        return
    P = n.parent
    if P.pointers_number() < Tree.n:
        i = P.pointers_number()
        while i >= 1:
            # find n and insert after n
            if P.pointers[i - 1] != n:
                P.pointers[i] = P.pointers[i - 1]
                i = i - 1
            else:
                P.pointers[i] = N
                break
        P.keys.insert(i - 1, value)
    else:
        tempNode = Node(Tree.n + 1, False)
        i = 0
        while i < len(P.keys):
            tempNode.pointers[i] = P.pointers[i]
            tempNode.keys.append(P.keys[i])
            i = i + 1
        tempNode.pointers[i] = P.pointers[i]
        i = tempNode.pointers_number()
        while i >= 1:
            if tempNode.pointers[i - 1] != n:
                tempNode.pointers[i] = tempNode.pointers[i - 1]
                i = i - 1
            else:
                tempNode.pointers[i] = N
                break
        tempNode.keys.insert(i - 1, value)
        P.erase_all()
        newP = Node(Tree.n, False)
        newP.parent = P.parent
        i = 0
        while i < math.ceil(Tree.n / 2) - 1:
            P.pointers[i] = tempNode.pointers[i]
            P.keys.append(tempNode.keys[i])
            tempNode.pointers[i].parent = P
            i = i + 1
        P.pointers[i] = tempNode.pointers[i]
        tempNode.pointers[i].parent = P
        newValue = tempNode.keys[i]
        i = i + 1
        while i < Tree.n:
            newP.pointers[i - math.ceil(Tree.n / 2)] = tempNode.pointers[i]
            newP.keys.append(tempNode.keys[i])
            tempNode.pointers[i].parent = newP
            i = i + 1
        newP.pointers[i - math.ceil(Tree.n / 2)] = tempNode.pointers[i]
        tempNode.pointers[i].parent = newP
        insert_in_parent(P, newValue, newP, Tree)


def insert(value, pointer, Tree):
    if Tree.root is None:
        Tree.root = Node(Tree.n, True)  # is leaf node
        current = Tree.root
    else:
        path = []  # record nodes
        # find the leaf node L that should contain key value
        current = Tree.root
        path.append(None)
        j = 0
        while not current.is_leaf:
            i = 0
            while i < len(current.keys):
                if value > current.keys[i]:  # smallest greater
                    i = i + 1
                else:
                    break
            path.append(current)  # add next's parent
            j = j + 1
            if i == len(current.keys):  # no such i
                # find the last non-null pointer in the node
                k = len(current.pointers) - 1
                while k >= 0:
                    if current.pointers[k] is not None:
                        break
                    k = k - 1
                current = current.pointers[k]  # last pointer
            elif value == current.keys[i]:
                current = current.pointers[i + 1]
            else:
                current = current.pointers[i]  # value < current.key[i]
        #  current is the leaf node to be inserted
    if len(current.keys) < Tree.n - 1:
        insert_in_leaf(current, value, pointer, Tree)  # here pointer is the address of the record
    else:
        newNode = Node(Tree.n, True)
        newNode.parent = current.parent
        tempNode = Node(Tree.n + 1, False)
        i = 0
        while i < len(current.keys):
            tempNode.keys.append(current.keys[i])
            tempNode.pointers[i] = current.pointers[i]
            i = i + 1
        insert_in_leaf(tempNode, value, pointer, Tree)
        newNode.pointers[Tree.n - 1] = current.pointers[Tree.n - 1]
        current.pointers[Tree.n - 1] = newNode
        current.erase()
        i = 0
        while i < math.ceil(Tree.n / 2):
            current.keys.append(tempNode.keys[i])
            current.pointers[i] = tempNode.pointers[i]
            i = i + 1
        while i < Tree.n:
            newNode.keys.append(tempNode.keys[i])
            newNode.pointers[i - math.ceil(Tree.n / 2)] = tempNode.pointers[i]
            i = i + 1
        # find the smallest value in newNode
        smallest = newNode.keys[0]
        i = 0
        while i < len(newNode.keys):
            if smallest > newNode.keys[i]:
                smallest = newNode.keys[i]
            i = i + 1
        insert_in_parent(current, smallest, newNode, Tree)


def swap_variables(N, Np):
    tempNode = Node(N.n, False)
    tempNode.keys = N.keys[:]
    tempNode.pointers = N.pointers[:]
    N.keys = Np.keys[:]
    N.pointers = Np.pointers[:]
    Np.keys = tempNode.keys[:]
    Np.pointers = tempNode.pointers[:]
    if N.is_leaf:
        return
    # need to change parent
    j = 0
    while j < N.pointers_number():
        N.pointers[j].parent = N
        j = j + 1
    j = 0
    while j < Np.pointers_number():
        Np.pointers[j].parent = Np
        j = j + 1


def delete_entry(N, value, pointer, Tree):
    # delete (value, pointer) from N
    # delete value(key)
    i = N.pointers.index(pointer)
    value_i = N.keys.index(value)
    N.keys.pop(value_i)
    # delete pointers and shift left
    j = i
    if not N.is_leaf:
        while j <= Tree.n - 2:
            N.pointers[j] = N.pointers[j + 1]
            j = j + 1
        N.pointers[j] = None
    else:
        while j < Tree.n - 2:
            N.pointers[j] = N.pointers[j + 1]
            j = j + 1
        N.pointers[j] = None
    # adjust
    if N == Tree.root and not N.is_leaf and N.pointers_number() == 1:
        Tree.root = N.pointers[0]
        N.pointers[0].parent = None
    elif N == Tree.root and N.is_leaf:
        return
    elif N == Tree.root and not N.is_leaf:
        return
    else:  # too few values/pointers
        pt = N.pointers_number()
        if (not N.is_leaf and pt < math.ceil(Tree.n / 2)) or (
                N.is_leaf and len(N.keys) < math.ceil((Tree.n - 1) / 2)):
            P = N.parent
            # find the previous or next child of N.parent
            # KP is the value between N and NP in P
            N_index = P.pointers.index(N)
            previousFlag = False
            nextFlag = False
            # previous first
            if N_index != 0:
                Np = P.pointers[N_index - 1]
                Kp = P.keys[N_index - 1]
                previousFlag = True
            elif P.pointers[N_index + 1] is not None:
                Np = P.pointers[N_index + 1]
                Kp = P.keys[N_index]
                nextFlag = True
            else:
                print("find adjacent error!")
                return None
            # check if N and Np can fit in a single node
            if (N.is_leaf and (N.pointers_number() + Np.pointers_number() - 1 <= Tree.n)) or (
                    not N.is_leaf and N.pointers_number() + Np.pointers_number() <= Tree.n):
                # coalesce nodes
                if nextFlag:
                    # N is predecessor of Np, swap
                    swap_variables(N, Np)
                if not N.is_leaf:
                    j = 0
                    length = Np.pointers_number()
                    while j < N.pointers_number():
                        if j == 0:
                            Np.keys.append(Kp)
                        else:
                            Np.keys.append(N.keys[j - 1])
                        Np.pointers[length + j] = N.pointers[j]
                        N.pointers[j].parent = Np
                        j = j + 1
                else:
                    # append all (Ki, Pi) pairs in N to Np, set Np.Pn = N.Pn
                    j = 0
                    length = Np.pointers_number()
                    while N.pointers[j] is not None:
                        Np.pointers[length - 1 + j] = N.pointers[j]
                        Np.keys.append(N.keys[j])
                        j = j + 1
                    Np.pointers[Tree.n - 1] = N.pointers[Tree.n - 1]

                delete_entry(N.parent, Kp, N, Tree)
            else:
                # cannot fit in one node, redistribution
                if previousFlag:
                    if not N.is_leaf:
                        m = Np.pointers_number() - 1
                        Np_pm = Np.pointers[m]
                        Np.pointers[m] = None
                        Np_km_1 = Np.keys[m - 1]
                        Np.keys.pop(m - 1)
                        N.keys.insert(0, Np_km_1)
                        N.pointers.insert(0, Np_pm)
                        N.pointers[0].parent = N
                        N.pointers[Tree.n - 1] = N.pointers[Tree.n]
                        N.pointers.pop()
                        P.keys[P.keys.index(Kp)] = Np_km_1
                    else:
                        m = len(Np.keys)
                        Np_pm = Np.pointers[m - 1]
                        Np.pointers[m - 1] = None
                        Np_km = Np.keys[m - 1]
                        Np.keys.pop(m - 1)
                        N.keys.insert(0, Np_km)
                        N.pointers.insert(0, Np_pm)
                        N.pointers[Tree.n - 1] = N.pointers[Tree.n]
                        N.pointers.pop()
                        P.keys[P.keys.index(Kp)] = Np_km
                else:
                    if not N.is_leaf:
                        Np_p0 = Np.pointers[0]
                        Np.pointers.pop(0)
                        Np.pointers.append(None)
                        Np_k0 = Np.keys[0]
                        Np.keys.pop(0)
                        N.keys.append(Kp)
                        N.pointers[N.pointers_number()] = Np_p0
                        Np_p0.parent = N
                        P.keys[P.keys.index(Kp)] = Np_k0
                    else:
                        Np_p0 = Np.pointers[0]
                        Np.pointers.pop(0)
                        Np.pointers.append(None)
                        Np.pointers[Tree.n - 1] = Np.pointers[Tree.n - 2]
                        Np.pointers[Tree.n - 2] = None
                        Np_k0 = Np.keys[0]
                        Np.keys.pop(0)
                        N.keys.append(Np_k0)
                        N.pointers.insert(N.pointers_number() - 1, Np_p0)
                        P.keys[P.keys.index(Kp)] = Np.keys[0]


def delete(value, pointer, Tree):
    # find the leaf node L that contains (value, pointer), if there are duplicate key, this
    # look up algorithm will find the leftmost leaf node
    current, i = find(value, Tree)
    done = False
    if current is None:
        return None  # not found
    else:
        while not done and current is not None:
            while i < len(current.keys) and current.keys[i] == value:
                if current.pointers[i] == pointer:
                    break
                else:
                    i = i + 1
            if i >= len(current.keys):
                current = current.pointers[i]
                i = 0
            elif current.keys[i] != value:
                return None  # not found
            else:
                done = True
        if current is None:
            return None
    return delete_entry(current, value, pointer, Tree)


# This function return the leaf node where the minimum key are stored, and None if empty
def get_mini_record(Tree):
    if Tree.root is None:
        return None
    current = Tree.root
    while isinstance(current.pointers[0], Node):
        current = current.pointers[0]
    if current.pointers[0] is None:
        return None  # The root is not None but is empty
    else:
        return current


def calculate_n(size):
    return math.floor(4 * 1024 / (size + 64))

if __name__ == "__main__":
    # demo
    n = calculate_n(4)
    Tree = Index(n, None, "ID")
    key_value = 0
    while key_value < 2000:
        insert(key_value, str(key_value), Tree)
        key_value = key_value + 1

    print(get_mini_record(Tree).pointers[0])
    print(get_mini_record(Tree).pointers[1])

    key_value = 200
    while key_value < 800:
        delete(key_value, str(key_value), Tree)
        key_value = key_value + 1

    num = 0
    while num < 1000:
        node, i = find(num, Tree)
        record = recordAll(node, i, num)
        print(record)
        num = num + 1


