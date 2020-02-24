# To RUN this script run
# . st.sh 
# the above runs the scripts in the same shell as the CLI
# nornally a shell script runs in a sub shell and does
# not impact the CLI shell it is executed in

# Run the following only once 
# pip install --upgrade virtualenv
# virtualenv .
#



. ./bin/activate
## to deactivate run $ deactivate
# Run only once
# pip install apache-beam
# pip install apache-beam[test]

python -m examples.amit.wc
