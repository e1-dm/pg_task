from bs4 import BeautifulSoup
import urllib2
import numpy
import json
import time
import multiprocessing.dummy as mp
import os.path


class DataHandler:
    def __init__(self, link, count, correlation=50):
        self.alpha = 0.85
        self.correlation = correlation
        self.starting_link = link
        self.total_links = count
        self.connections = {}
        self.time_spent = [0, 0, 0, 0]
        self.page_ranks = []
        self.adjacency_matrix = numpy.zeros(shape=(self.total_links, self.total_links))
        self.sparse_matrix = {}
        self.steps = ["Collecting links", "Checking connections", "Creating adjacency matrix",
                      "Computation of PR"]

        self.print_step(1)
        start_time = time.time()
        if os.path.isfile('connections.json'):
            with open('connections.json', 'r') as fp:
                self.connections = json.load(fp)
                self.links = sorted(self.connections.keys())
                fp.close()
            print "Loaded connections from connections.json"
            self.time_spent[1] = time.time() - start_time
            self.print_time_spent(1)
        else:
            self.links = self.collect_links(self.starting_link)[1][:99]
            self.links.append(self.starting_link)
            self.get_connections_parallel()

        print "List: ", self.links
        self.form_adjacency_matrix()
        self.save_matrix_to_file(self.adjacency_matrix)
        self.compute_am_page_rank()
        # self.form_sparse_matrix()
        # self.compute_sm_page_rank()
        # self.compute_sm_page_rank_parallel()

    @staticmethod
    def collect_links(starting_link, constraint=0):
        resp = urllib2.urlopen(starting_link)
        soup = BeautifulSoup(resp, "html.parser")
        links_set = set()

        for link in soup.find_all('a', href=True):
            if constraint and len(links_set) == constraint:
                break
            if link is not None:
                current_link = link.get('href')
                if not current_link.endswith(('jpg', 'png')) and not (current_link + '/') in links_set and \
                        (current_link.startswith('http') or current_link.startswith('/') or current_link.startswith(
                            'www')) and current_link != starting_link:
                    links_set.add(current_link)

        return (starting_link, list(links_set))

    def print_step(self, index):
        print "==================="
        print self.steps[index]

    def print_time_spent(self, index):
        print "Time spent: ", self.time_spent[index]

    def get_connections(self):
        start_time = time.time()
        for link in self.links:
            self.connections[link] = self.collect_links(link)[1]

        self.save_to_json()
        self.time_spent[1] = time.time() - start_time
        self.print_time_spent(1)

    def get_connections_parallel(self, number_of_p=32):
        start_time = time.time()
        pool = mp.Pool(number_of_p)
        self.connections = dict(pool.map(self.collect_links, self.links))
        self.save_to_json()
        self.time_spent[1] = time.time() - start_time
        self.print_time_spent(1)

    def save_to_json(self):
        with open('connections.json', 'w') as fp:
            json.dump(self.connections, fp)

    def form_adjacency_matrix(self):
        self.print_step(2)
        start_time = time.time()
        for link, connections in self.connections.items():
            initial_link_index = self.links.index(link)
            for following_link in connections:
                if following_link in self.links:
                    self.adjacency_matrix[initial_link_index, self.links.index(following_link)] = 1

        self.time_spent[2] = time.time() - start_time
        self.print_time_spent(2)

    def form_sparse_matrix(self):
        print "==================="
        print "Creating sparse matrix"
        for i in xrange(len(self.links)):
            self.sparse_matrix[i] = []

        start_time = time.time()
        for link, connections in self.connections.items():
            initial_link_index = self.links.index(link)
            for following_link in connections:
                if following_link in self.links:
                    following_link_index = self.links.index(following_link)
                    self.sparse_matrix[following_link_index].append(initial_link_index)

        self.time_spent[2] = time.time() - start_time
        self.print_time_spent(2)

    def save_matrix_to_file(self, matrix):
        if not os.path.isfile('matrix.txt'):
            max_l = len(max(self.links, key=len))
            f = open("matrix.txt", "w")
            for link in self.links:
                f.write(link + " ")
            f.write("\r")
            for i, row in enumerate(matrix):
                f.write(self.links[i] + " " * (1+ max_l - len(self.links[i])))
                for value in row:
                    f.write(str(value) + " ")
                f.write("\r")
            f.close()

    def compute_am_page_rank(self):
        self.print_step(3)
        start_time = time.time()
        self.page_ranks = numpy.ones(len(self.links))

        for i in xrange(self.correlation):
            for j, link in enumerate(self.links):
                column_sum = 0
                for t, column_value in enumerate(self.adjacency_matrix[:, j]):
                    if column_value != 0:
                        column_sum += self.page_ranks[t] / \
                                      numpy.sum(self.adjacency_matrix[:, t])

                self.page_ranks[j] = (1 - self.alpha) / len(self.links) + self.alpha * column_sum
        # print self.page_ranks
        for pair in sorted(zip(self.links, self.page_ranks), key=lambda x: x[1]):
            print str(pair) + "\n"

        self.time_spent[3] = time.time() - start_time
        self.print_time_spent(3)

    def count_sum(self, index):
        for link in self.sparse_matrix[index]:
            self.sum_values[index] += self.page_ranks[link] / float(len(self.sparse_matrix[link]))

    def compute_sm_page_rank(self):
        self.print_step(3)
        start_time = time.time()
        self.page_ranks = numpy.ones(len(self.links))
        self.sum_values = numpy.zeros(len(self.links))
        for i in xrange(self.correlation):
            for index, linked in self.sparse_matrix.items():
                for link in linked:
                    self.sum_values[index] += self.page_ranks[link] / float(len(self.sparse_matrix[link]))

            for j in xrange(len(self.links)):
                self.page_ranks[j] = (1 - self.alpha) / len(self.links) + self.alpha * self.sum_values[j]
                self.sum_values[j] = 0
        for pair in sorted(zip(self.links, self.page_ranks), key=lambda x: x[1]):
            print str(pair) + "\n"
        self.time_spent[3] = time.time() - start_time
        self.print_time_spent(3)

    def compute_sm_page_rank_parallel(self, number_of_p=4):
        self.print_step(3)
        start_time = time.time()
        self.page_ranks = numpy.ones(len(self.links))
        self.sum_values = numpy.zeros(len(self.links))
        for i in xrange(self.correlation):
            pool = mp.Pool(number_of_p)
            pool.map(self.count_sum, self.sparse_matrix.keys())

            for j in xrange(len(self.links)):
                self.page_ranks[j] = (1 - self.alpha) / len(self.links) + self.alpha * self.sum_values[j]
                self.sum_values[j] = 0
        for pair in sorted(zip(self.links, self.page_ranks), key=lambda x: x[1]):
            print str(pair) + "\n"
        self.time_spent[3] = time.time() - start_time
        self.print_time_spent(3)
