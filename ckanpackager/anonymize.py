#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
This script contains a command line program which anonymizes an existing database of non-anonymized
data. It should only be run once when changing the `ANONYMIZE_EMAILS` setting from False (or
non-existant) to True. Running it multiple times will cause problems and would be impossible to
unpick due to the one way hashing of the email addresses (the whole point really!).

To run, simply run this script from the ckanpackager's virtualenv or indeed whatever python env it
is installed into:

    `python anonymize.py --help`

See help information for details of parameters.
"""

import argparse

import dataset

from ckanpackager.lib.statistics import anonymize_email, extract_domain


class Anonymizer(object):
    """
    Class for anonymizing existing data in the ckanpackager's stats database. This should be only be
    run if the `ANONYMIZE_EMAILS` config flag has been set to True after data has been inserted into
    a database whilst it was False (or indeed not present at all).
    """

    def __init__(self, database_url, dry_run):
        """
        :param database_url: the database url for the dataset lib, this should be in an sqlalchemy
                             compatible form
        :param dry_run: if True no modifications are made to the database and what would have been
                        done is logged
        """
        self.database = dataset.connect(database_url)
        self.dry_run = dry_run
        # cache the hashes of the email addresses we cache to avoid wasting time repeating work
        # we've already done
        self.hash_cache = {}

    def get_hashed_email(self, email):
        """
        Given an email address, hash it and return the hash. This will use the hash cache if it can.

        :param email: the email address
        :return: the hashed email address
        """
        if email not in self.hash_cache:
            self.hash_cache[email] = anonymize_email(email)
        return self.hash_cache[email]

    def get_requests_rows(self):
        """
        Returns a list of dicts which represent updates to be applied to each row of the requests
        table. The update includes the domain and the hashed email address from the original row as
        well as the id of the row so that it is known which row is meant to be updated with the
        data.

        :return: a list of row dicts ready for dataset's update function
        """
        rows = []
        for row in self.database[u'requests'].all():
            # make sure we are working with the lowercase email address to avoid duplication due to
            # different case
            email = row[u'email'].lower()
            domain = extract_domain(email)
            hashed_email = self.get_hashed_email(email)
            row = {
                u'id': row[u'id'],
                u'email': hashed_email,
                u'domain': domain
            }
            rows.append(row)
        return rows

    def get_errors_rows(self):
        """
        Returns a list of dicts which represent updates to be applied to each row of the errors
        table. The update includes the hashed email address from the original row as well as the id
        of the row so that it is known which row is meant to be updated with the data.

        :return: a list of row dicts ready for dataset's update function
        """
        rows = []
        for row in self.database[u'errors'].all():
            # make sure we are working with the lowercase email address to avoid duplication due to
            # different case
            email = row[u'email'].lower()
            hashed_email = self.get_hashed_email(email)
            row = {
                u'id': row[u'id'],
                u'email': hashed_email
            }
            rows.append(row)
        return rows

    def run(self):
        """
        Iterate over the rows in the requests and errors tables, replacing email addresses with
        hashed versions. The requests table will also have a new column added to store the domain
        from the email address to allow us to produce statistics based on the domains of requesters.
        """
        updates = {
            u'requests': self.get_requests_rows(),
            u'errors': self.get_errors_rows(),
        }
        for table, rows in updates.items():
            for row in rows:
                if self.dry_run:
                    print u'Would have updated {} with {}'.format(table, row)
                else:
                    self.database[table].update(row, [u'id'])


if __name__ == u'__main__':
    description = u'''Anonymize the data in the ckanpackager database.
                      This is a one way process and cannot be undone so be careful! This script
                      should only be run once, when changing the `ANONYMIZE_EMAILS` setting from
                      False (or non-existant) to True. Take care!'''
    database_url_help = u'''an sqlalchemy compatible database url for the ckanpackager's stats
                            database to to be anonymized'''
    dry_run_help = u'''Don't run the anonymization, just log about what would be done'''

    parser = argparse.ArgumentParser(description=description)
    # we could get this from the config but then we'd have to import it correctly or fire up a whole
    # flask app just to get to it. It makes more sense to just ask the user to pass it, particularly
    # given this command should only need to be run once upon changing the `ANONYMIZE_EMAILS`
    # setting
    parser.add_argument(u'database_url', type=unicode, help=database_url_help)
    parser.add_argument(u'--dry_run', u'-d', action=u'store_true', help=dry_run_help)

    args = parser.parse_args()

    anonymizer = Anonymizer(**vars(args))
    anonymizer.run()
